"""Seed the task queue from a task-parameter feather.

This is the queue *producer*: it reads the feather built by
``build_tasks.py`` (one row per ``root_id``, columns matching the
``_run_task`` signature in ``src/catacombs_task/task.py``), turns each row into
a queued task, and inserts them into the queue at ``TASKQUEUE_PATH``. Workers
(``run.py``) later pull and run them.

Configuration comes entirely from the environment, layered by the ``poe`` task
as ``config/common.env`` + ``config/<profile>/task.env`` (see
``config/README.md``):

    CF_CLOUDPATH              CloudFiles output dir; also used to skip finished tasks
    TASKQUEUE_PATH            queue to insert into (SQS URL, fq:// path, …)

The task feather is located by ``task_feather_path(profile)`` — the same path
``build_tasks.py`` writes — so it is derived from the profile name and needs no
env var (set ``TASK_PARAMETER_FILENAME`` in task.env only to override it).

Run it with a profile:  ``poe insert_tasks -e <profile>``.
"""

import argparse
import os

import pandas as pd
from taskqueue import TaskQueue

from catacombs_task.paths import task_feather_path
from catacombs_task.task import filter_tasks, run_task


def load_task_df(parameter_filename: str, cloudpath: str) -> pd.DataFrame:
    """Read the task feather and stamp the shared ``cloudpath`` onto each row.

    ``cloudpath`` is constant across a run, so it is not stored in the feather;
    it is injected here (unless the feather already provides a per-row value)
    because ``_run_task`` takes it as a keyword argument.
    """
    task_df = pd.read_feather(parameter_filename)
    if "cloudpath" not in task_df.columns:
        task_df["cloudpath"] = cloudpath
    return task_df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", help="Config profile under config/ (e.g. v1dd, microns)"
    )
    profile = parser.parse_args().profile

    parameter_filename = task_feather_path(profile)

    target_cloudpath = os.environ.get("CF_CLOUDPATH")
    if target_cloudpath is None:
        raise ValueError("Must set CF_CLOUDPATH")

    if os.environ.get("TASKQUEUE_PATH") is None:
        raise ValueError("Must set TASKQUEUE_PATH")

    task_df = load_task_df(parameter_filename, target_cloudpath)

    # Skip neurons already assembled in a previous run so reruns only enqueue
    # the ones still missing an output (see filter_tasks in task.py).
    original_number = len(task_df)
    task_df = filter_tasks(task_df, target_cloudpath)
    print(f"Inserting {len(task_df)}/{original_number} tasks!")

    if len(task_df) == 0:
        print("No tasks to insert!")
        return

    # Each row's columns become keyword arguments to _run_task.
    task_args = task_df.to_dict(orient="records")
    tq = TaskQueue(os.environ.get("TASKQUEUE_PATH"))
    tasks = (run_task(**task) for task in task_args)
    tq.insert(tasks, total=len(task_args))


if __name__ == "__main__":
    main()
