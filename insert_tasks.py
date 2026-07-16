"""Seed the task queue from a task-parameter feather.

This is the queue *producer*: it reads the feather built by
``build_tasks.py`` (one row per ``root_id``, columns matching the
``_run_task`` signature in ``src/catacombs_task/task.py``), groups the rows into
batches of ``TASK_BATCH_SIZE`` cells, turns each batch into a queued task, and
inserts them into the queue at ``TASKQUEUE_PATH``. Workers (``run.py``) later
pull and run them, loading each batch in one pooled ossify call.

Configuration comes entirely from the environment, layered by the ``poe`` task
as ``config/common.env`` + ``config/<profile>/task.env`` (see
``config/README.md``):

    CF_CLOUDPATH              CloudFiles output dir; also used to skip finished tasks
    TASKQUEUE_PATH            queue to insert into (SQS URL, fq:// path, …)
    TASK_BATCH_SIZE           cells per queued task (default 10)

The task feather is located by ``task_feather_path(profile)`` — the same path
``build_tasks.py`` writes — so it is derived from the profile name and needs no
env var (set ``TASK_PARAMETER_FILENAME`` in task.env only to override it).

Run it with a profile:  ``poe insert_tasks -e <profile>``.
"""

import argparse
import os
import sys

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
    parser.add_argument(
        "--no-ask",
        action="store_true",
        help="Skip the confirmation prompt and insert immediately (for scripting). "
        "Via poe: `poe insert_tasks -e <profile> -- --no-ask`.",
    )
    args = parser.parse_args()
    profile = args.profile

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

    # Echo the effective, baked-in parameters before seeding the queue. cloudpath
    # and the queue URL are frozen into each task here (not read from the env at
    # run time), so a stale CF_CLOUDPATH silently misdirects every output and its
    # error sidecar. Surfacing them here is the cheapest guard against that drift.
    # Cells are grouped into batches of BATCH_SIZE cells per queued task; the
    # worker loads each batch in one pooled ossify call (see task.py). N=10 is
    # the measured throughput sweet spot; override via TASK_BATCH_SIZE.
    batch_size = int(os.environ.get("TASK_BATCH_SIZE", "10"))
    n_batches = -(-len(task_df) // batch_size)  # ceil division

    datastack = task_df["datastack_name"].iloc[0] if len(task_df) else "(n/a)"
    print("── insert_tasks ──────────────────────────────────────")
    print(f"  profile   : {profile}")
    print(f"  queue     : {os.environ.get('TASKQUEUE_PATH')}")
    print(f"  cloudpath : {target_cloudpath}")
    print(f"  datastack : {datastack}")
    print(f"  feather   : {parameter_filename}")
    print(
        f"  cells     : {len(task_df)} to insert "
        f"({original_number - len(task_df)} already done, skipped)"
    )
    print(f"  tasks     : {n_batches} batches of ≤{batch_size} cells")
    print("───────────────────────────────────────────────────────")

    if len(task_df) == 0:
        print("No tasks to insert!")
        return

    # Confirm before seeding the queue — the parameters above are frozen into
    # every task, so this is the last chance to catch a stale cloudpath/queue.
    # --no-ask bypasses for scripted runs; a non-interactive shell without it
    # refuses rather than block on input() with no TTY.
    if not args.no_ask:
        if not sys.stdin.isatty():
            print(
                "Non-interactive shell: re-run with --no-ask to insert without "
                "confirmation."
            )
            return
        if input("Proceed with insert? [y/N] ").strip().lower() not in ("y", "yes"):
            print("Aborted; nothing inserted.")
            return

    # Group the one-row-per-cell records into batches of `batch_size`. Every
    # column except root_id is a constant broadcast across the run (see
    # build_tasks.py), so the first record of a chunk carries the shared kwargs;
    # root_id is collected into the `root_ids` list _run_task expects.
    records = task_df.to_dict(orient="records")
    batches = [records[i : i + batch_size] for i in range(0, len(records), batch_size)]

    def _payload(chunk):
        shared = {k: v for k, v in chunk[0].items() if k != "root_id"}
        return run_task(root_ids=[int(r["root_id"]) for r in chunk], **shared)

    tq = TaskQueue(os.environ.get("TASKQUEUE_PATH"))
    tasks = (_payload(chunk) for chunk in batches)
    tq.insert(tasks, total=len(batches))


if __name__ == "__main__":
    main()
