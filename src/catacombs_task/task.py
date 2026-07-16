"""Task-queue plumbing for assembling ossify skeletons.

This module intentionally holds *no* ossify logic — that lives in
``processing.py``. Here we only:

- adapt the work into a ``@taskqueue.queueable`` unit (``_run_task``),
- wrap it as a partial for the queue producer (``run_task``),
- decide which tasks can be skipped (``filter_tasks``).

Each task is one *batch* of ``root_ids`` (``insert_tasks.py`` groups the feather's
one-row-per-cell records into chunks of ``TASK_BATCH_SIZE``). The batch loader
pools the per-cell fetches into a few queries; the worker then processes each
returned cell independently so one bad cell never sinks the rest of the batch.
The shared per-task parameters come from the columns of the task dataframe (see
``build_tasks.py``): every column name except ``root_id`` is passed as a keyword
argument to ``_run_task`` (``root_id`` becomes the ``root_ids`` list).
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
    assemble_cell_batch,
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

# Permanent, data-caused failure markers follow the convention
# "{root_id}.err.<reason>": the id was tried and will never succeed on retry
# (a property of the neuron, not a transient/server error). ``filter_tasks``
# treats any such marker as resolved, so reruns don't re-enqueue it, and it is
# NOT re-fetched. Add new reasons freely — the machinery keys off the "err"
# segment, not the specific reason.
ERR_PREFIX = "err"
# Reasons wired up so far:
#  - singlet: single-vertex skeletons, too small for ossify's graph smoothing
#    ("{root_id}.err.singlet").
#  - invalid: a root id not valid at the batch's shared timestamp (a stale /
#    outdated segment id — a data property, not a transient error), so it is
#    dropped from the batch and marked permanently ("{root_id}.err.invalid").
SINGLET_EXTENSION = f"{ERR_PREFIX}.singlet"
INVALID_EXTENSION = f"{ERR_PREFIX}.invalid"


def is_resolved(filename: str) -> bool:
    """Whether an output-dir artifact means its root_id is done and must not be
    re-enqueued: a ``.osy`` output (success) or any ``{root_id}.err.<reason>``
    marker (permanent, data-caused failure). ``*_error.json`` sidecars are
    transient/retryable and are NOT resolved.
    """
    if filename.endswith(f".{OSSIFY_EXTENSION}"):
        return True
    parts = filename.split(".")  # "<root_id>.err.<reason>"; root_ids have no dots
    return len(parts) >= 3 and parts[1] == ERR_PREFIX


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

    A neuron is resolved once it has either its output ``.osy`` (assembled) or
    any ``{root_id}.err.<reason>`` marker (tried, but a permanent data-caused
    failure — e.g. ``.err.singlet``). Used by ``insert_tasks.py`` so a rerun
    only enqueues neurons that have not resolved yet (errors leave a
    ``*_error.json`` sidecar, which is NOT resolved, so failed neurons retry).
    """
    cf = CloudFiles(cloudpath)
    done_ids = np.array([int(f.split(".")[0]) for f in cf.list() if is_resolved(f)])
    if len(done_ids) == 0:
        return task_df
    return task_df[~task_df["root_id"].isin(done_ids)]


def _is_pending(cf, root_id: int) -> bool:
    """Whether ``root_id`` still needs processing in this output dir.

    A secondary, per-worker idempotency guard on top of ``filter_tasks`` (which
    dedups at insert time): if a batch is re-leased after a partial run, this
    skips the cells that already resolved. Resolved means a ``.osy`` output or
    one of the permanent ``.err.<reason>`` markers a worker writes.
    """
    if cf.exists(f"{root_id}.{OSSIFY_EXTENSION}"):
        return False
    if cf.exists(f"{root_id}.{SINGLET_EXTENSION}"):
        return False
    if cf.exists(f"{root_id}.{INVALID_EXTENSION}"):
        return False
    return True


@taskqueue.queueable
def _run_task(
    root_ids: list[int],
    cloudpath: str,
    datastack_name: str,
    server_address: str,
    restore_properties: bool = True,
    reference_tables: Optional[list[str]] = None,
    reference_suffixes: Optional[dict] = None,
    compartment_model: Optional[str] = None,
    skeleton_version: int = 4,
    materialization_version: Optional[int] = None,
):
    """Assemble a batch of neurons' ossify cells and upload one ``.osy`` each.

    The batch is loaded in one pooled call (``assemble_cell_batch``); each
    returned cell is then processed independently. Failures are caught and
    recorded per cell (JSON sidecars for transient errors, ``.err.<reason>``
    markers for permanent ones) rather than raised, so a single bad neuron — or
    a root that is invalid / not-yet-skeletonized — does not kill the batch or
    the worker. Absence of ``{root_id}.osy`` (and of any ``.err`` marker) is the
    signal that a neuron still needs processing (see ``filter_tasks``).
    """
    cf = CloudFiles(cloudpath, progress=False)

    # taskqueue may hand back a single scalar or a numpy array depending on how
    # the payload was serialized; normalize to a plain list of python ints.
    root_ids = [int(r) for r in np.atleast_1d(root_ids)]

    # Idempotency: only process root_ids not already resolved (see _is_pending).
    pending = [rid for rid in root_ids if _is_pending(cf, rid)]
    if not pending:
        return

    # Pin the materialization version stamped in at build time (coerced to a
    # plain int — the version setter rejects numpy ints). This locks the query
    # timestamp so every batch across the run shares one consistent snapshot;
    # None falls back to the latest version.
    client = CAVEclient(
        datastack_name,
        server_address=server_address,
        version=int(materialization_version)
        if materialization_version is not None
        else None,
    )

    # One shared timestamp for the whole batch (the pinned version's frozen
    # timestamp when set): used both to load the cells and (below) to classify
    # any roots the loader drops, so the two agree.
    ts = client.materialize.get_timestamp()

    try:
        cells = assemble_cell_batch(
            pending,
            client,
            restore_properties=restore_properties,
            reference_tables=(
                list(reference_tables) if reference_tables is not None else None
            ),
            reference_suffixes=(
                dict(reference_suffixes) if reference_suffixes is not None else None
            ),
            skeleton_version=skeleton_version,
            timestamp=ts,
        )
    except Exception as e:
        # A whole-batch load failure (e.g. a pooled synapse query error) is
        # transient: record a retryable sidecar for every pending root so a
        # later insert_tasks re-enqueues them, and bail.
        for rid in pending:
            _record_error(cf, f"{rid}.{OSSIFY_EXTENSION}", "skeletonization", rid, e)
        return

    # Roots the batch loader dropped (absent from the result). Classify them with
    # the same shared timestamp: not-latest is a permanent, data-caused failure
    # (mark it so reruns skip it); latest-but-missing means the skeleton has not
    # been generated yet (transient — leave it unmarked so a rerun retries once
    # the generate_skeletons pre-pass catches up).
    missing = [rid for rid in pending if rid not in cells]
    if missing:
        valid_mask = client.chunkedgraph.is_latest_roots(missing, timestamp=ts)
        for rid, is_valid in zip(missing, valid_mask):
            if not is_valid:
                cf.put(f"{rid}.{INVALID_EXTENSION}", b"")
            else:
                print(
                    f"[{rid}] skeleton not available yet (run generate_skeletons "
                    "and wait); leaving for retry.",
                    file=sys.stderr,
                )

    for rid, cell in cells.items():
        filename = f"{rid}.{OSSIFY_EXTENSION}"
        try:
            # Single-vertex skeletons are too small for any downstream analysis
            # (and crash ossify's graph smoothing). Mark that this id was tried
            # and skip it; the marker keeps reruns from re-fetching it.
            if len(cell.s.vertex_df) <= 1:
                cf.put(f"{rid}.{SINGLET_EXTENSION}", b"")
                continue
            cell = extract_features(cell)
            if compartment_model:
                cell = assign_compartments(cell, compartment_model)
        except Exception as e:
            _record_error(cf, filename, "skeletonization", rid, e)
            continue

        try:
            upload_cell(filename, cell, cloudpath)
        except Exception as e:
            _record_error(cf, filename, "upload", rid, e)


def run_task(*args, **kwargs):
    """You can leave this function alone.

    Returns a partial that the queue producer enqueues; the worker in ``run.py``
    later calls it. This indirection keeps task-queue details out of the real
    work in ``processing.py``.
    """
    return partial(_run_task, *args, **kwargs)
