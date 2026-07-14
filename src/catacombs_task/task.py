"""Task-queue plumbing for assembling ossify skeletons.

This module intentionally holds *no* ossify logic — that lives in
``processing.py``. Here we only:

- adapt the work into a ``@taskqueue.queueable`` unit (``_run_task``),
- wrap it as a partial for the queue producer (``run_task``),
- decide which tasks can be skipped (``filter_tasks``).

Each task is one ``root_id``. Parameters come from the columns of the task
dataframe (see ``insert_tasks.py``): every column name is passed as a keyword
argument to ``_run_task``.
"""

import sys
import traceback
from functools import partial
from typing import Optional

import numpy as np
import taskqueue
from caveclient import CAVEclient, session_config
from cloudfiles import CloudFiles

from catacombs_task.processing import (
    assemble_cell,
    assign_compartments,
    extract_features,
    upload_cell,
)

session_config.set_session_defaults(
    max_retries=10,
    pool_block=True,
    pool_maxsize=10,
    backoff_factor=1,
)

# Output artifact extension. One ".osy" file is written per completed neuron.
OSSIFY_EXTENSION = "osy"

# Marker extension for neurons skipped as single-vertex skeletons. A
# "{root_id}.err.singlet" file records that the id was tried and is too small to
# assemble (one vertex), so reruns don't re-fetch it (see ``filter_tasks``). The
# "err.<reason>" convention leaves room for other permanent-skip markers later.
SINGLET_EXTENSION = "err.singlet"


def _record_error(cf, filename: str, stage: str, root_id: int, exc: Exception) -> None:
    """Write a rich error sidecar and echo it to stderr.

    Captures the exception type and full traceback (not just ``str(exc)``) so a
    failure can be diagnosed from the ``{filename}_{stage}_error.json`` sidecar
    alone. Must be called from within the ``except`` block so ``format_exc``
    sees the active exception. Echoing to stderr surfaces the same detail in a
    live worker's logs (``poe launch_worker`` / pod logs).
    """
    tb = traceback.format_exc()
    print(
        f"[{root_id}] {stage} failed: {type(exc).__name__}: {exc}\n{tb}",
        file=sys.stderr,
    )
    cf.put_json(
        f"{filename}_{stage}_error.json",
        {
            "root_id": int(root_id),
            "stage": stage,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": tb,
        },
    )


def filter_tasks(task_df, cloudpath):
    """Drop tasks already resolved in ``cloudpath``.

    A neuron is resolved once it has either its output ``.osy`` (assembled) or a
    ``.singlet`` marker (tried, but a single-vertex skeleton too small to use).
    Used by ``insert_tasks.py`` so a rerun only enqueues neurons that have not
    resolved yet (errors leave a ``*_error.json`` sidecar, not either of those,
    so failed neurons are retried).
    """
    cf = CloudFiles(cloudpath)
    resolved = (f".{OSSIFY_EXTENSION}", f".{SINGLET_EXTENSION}")
    done_ids = np.array(
        [int(f.split(".")[0]) for f in cf.list() if f.endswith(resolved)]
    )
    if len(done_ids) == 0:
        return task_df
    return task_df[~task_df["root_id"].isin(done_ids)]


@taskqueue.queueable
def _run_task(
    root_id: int,
    cloudpath: str,
    datastack_name: str,
    server_address: str,
    restore_properties: bool = True,
    reference_tables: Optional[list[str]] = None,
    reference_suffixes: Optional[dict] = None,
    compartment_model: Optional[str] = None,
    skeleton_version: int = 4,
):
    """Assemble one neuron's ossify cell and upload it.

    Failures are caught and recorded as JSON sidecars rather than raised, so a
    single bad neuron does not kill the worker. Absence of ``{root_id}.osy`` is
    the signal that the neuron still needs processing (see ``filter_tasks``).
    """
    cf = CloudFiles(cloudpath, progress=False)
    filename = f"{root_id}.{OSSIFY_EXTENSION}"
    singlet_marker = f"{root_id}.{SINGLET_EXTENSION}"

    # Idempotency: skip neurons already assembled (.osy) or already found to be
    # a single-vertex skeleton (.singlet) in a previous run.
    if cf.exists(filename) or cf.exists(singlet_marker):
        return

    client = CAVEclient(datastack_name, server_address=server_address)

    try:
        cell = assemble_cell(
            root_id,
            client,
            restore_properties=restore_properties,
            reference_tables=(
                list(reference_tables) if reference_tables is not None else None
            ),
            reference_suffixes=(
                dict(reference_suffixes) if reference_suffixes is not None else None
            ),
            skeleton_version=skeleton_version,
        )
        # Single-vertex skeletons are too small for any downstream analysis (and
        # crash ossify's graph smoothing). Record a marker that this id was tried
        # and stop before feature extraction / upload; the marker keeps reruns
        # from re-fetching it (see ``filter_tasks``).
        if len(cell.s.vertex_df) <= 1:
            cf.put(singlet_marker, b"")
            return
        cell = extract_features(cell)
        if compartment_model:
            cell = assign_compartments(cell, compartment_model)
    except Exception as e:
        _record_error(cf, filename, "skeletonization", root_id, e)
        return

    try:
        upload_cell(filename, cell, cloudpath)
    except Exception as e:
        _record_error(cf, filename, "upload", root_id, e)
    return


def run_task(*args, **kwargs):
    """You can leave this function alone.

    Returns a partial that the queue producer enqueues; the worker in ``run.py``
    later calls it. This indirection keeps task-queue details out of the real
    work in ``processing.py``.
    """
    return partial(_run_task, *args, **kwargs)
