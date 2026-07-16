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

# Workers leave two kinds of mid-level marker, both of the form
# "{root_id}.<kind>.<reason>" (root_ids have no dots; reasons may):
#
#  - err     : a *permanent*, data-caused failure. The id was tried and will
#              never succeed on retry (a property of the neuron, not a
#              transient/server error). ``is_resolved`` treats any ``err`` marker
#              as done, so reruns don't re-enqueue it and it is not re-fetched.
#  - blocked : a *retryable* block on an unmet precondition. The id was skipped
#              this pass for a reason we expect to clear on a later run once the
#              dependency is ready. NOT resolved — reruns re-enqueue it — but the
#              marker records the id so a targeted follow-up can be rebuilt from
#              exactly these markers (e.g. ``skeleton_not_exist_ids`` →
#              generate_skeletons --from-markers).
#
# Add new reasons under either prefix freely — the machinery keys off the kind
# segment, not the specific reason.
ERR_PREFIX = "err"
BLOCKED_PREFIX = "blocked"

# err reasons wired up so far:
#  - singlet: single-vertex skeletons, too small for ossify's graph smoothing
#    ("{root_id}.err.singlet").
#  - invalid: a root id not valid at the batch's shared timestamp (a stale /
#    outdated segment id — a data property, not a transient error), so it is
#    dropped from the batch and marked permanently ("{root_id}.err.invalid").
#  - processing: a per-cell processing stage (feature extraction / compartment
#    assignment) raised a non-transient error — a deterministic property of this
#    neuron that will fail identically on retry ("{root_id}.err.processing").
SINGLET_EXTENSION = f"{ERR_PREFIX}.singlet"
INVALID_EXTENSION = f"{ERR_PREFIX}.invalid"
PROCESSING_ERROR_EXTENSION = f"{ERR_PREFIX}.processing"

# blocked reasons wired up so far:
#  - skeleton_not_exist: the root is valid but the CAVE skeleton service has not
#    produced its skeleton yet, so the download-only batch loader returned no
#    cell ("{root_id}.blocked.skeleton_not_exist"). Rebuild a targeted
#    generate_skeletons call from these (see ``skeleton_not_exist_ids``).
#  - server_error: a transient server/config error (a 503 storm, a timeout, a
#    dropped connection) hit while loading or processing the cell — retryable,
#    not a property of the neuron ("{root_id}.blocked.server_error"). The rich
#    {filename}_{stage}_error.json sidecar keeps the traceback for diagnosis.
SKELETON_NOT_EXIST_EXTENSION = f"{BLOCKED_PREFIX}.skeleton_not_exist"
SERVER_ERROR_EXTENSION = f"{BLOCKED_PREFIX}.server_error"

# Exception type names (``type(exc).__name__``) that are transient/server- or
# config-caused rather than a property of the neuron's data. ``_record_error``
# marks a failure of one of these ``.blocked.server_error`` (retryable) instead
# of leaving only a data-caused sidecar; anything not listed here is treated as
# data-caused. Extend as new transient types show up in a run.
#
# ServerIncompatibilityError is a caveclient API-version check; it usually rides
# along with a 503 storm (a bad version read during a degraded window) and clears
# on retry — hence "server". If it instead stays stuck and dominates, that's a
# real client/server version mismatch to fix, not wait.
SERVER_ERROR_TYPES = {
    "HTTPError",  # nearly all are "503 Service Temporarily Unavailable"
    "ConnectionError",
    "ConnectTimeout",
    "ReadTimeout",
    "Timeout",
    "ChunkedEncodingError",
    "ProtocolError",
    "MaxRetryError",
    "SSLError",
    "RemoteDisconnected",
    "IncompleteRead",
    "ServiceUnavailable",
    "TooManyRequests",
    "GatewayTimeout",
    "ServerIncompatibilityError",
}


def marker_parts(filename: str) -> tuple[Optional[str], Optional[str]]:
    """``(kind, reason)`` for a ``{root_id}.<kind>.<reason>`` marker whose kind is
    ``ERR_PREFIX`` or ``BLOCKED_PREFIX``, else ``(None, None)``.

    root_ids contain no dots, so the reason is everything after the kind segment
    (reasons themselves may contain dots).
    """
    parts = filename.split(".")
    if len(parts) >= 3 and parts[1] in (ERR_PREFIX, BLOCKED_PREFIX):
        return parts[1], ".".join(parts[2:])
    return None, None


def is_resolved(filename: str) -> bool:
    """Whether an output-dir artifact means its root_id is done and must not be
    re-enqueued: a ``.osy`` output (success) or any ``{root_id}.err.<reason>``
    marker (permanent, data-caused failure). ``{root_id}.blocked.<reason>``
    markers (retryable, e.g. ``.blocked.skeleton_not_exist``) and ``*_error.json``
    sidecars are transient/retryable and are NOT resolved.
    """
    if filename.endswith(f".{OSSIFY_EXTENSION}"):
        return True
    kind, _ = marker_parts(filename)
    return kind == ERR_PREFIX


def _record_error(
    cf,
    filename: str,
    stage: str,
    root_id: int,
    exc: Exception,
    permanent_if_data: bool = False,
) -> None:
    """Write a rich error sidecar, stamp a state marker, and echo to stderr.

    Captures the exception type and full traceback (not just ``str(exc)``) so a
    failure can be diagnosed from the ``{filename}_{stage}_error.json`` sidecar
    alone. Must be called from within the ``except`` block so ``format_exc``
    sees the active exception. Echoing to stderr surfaces the same detail in a
    live worker's logs (``poe launch_worker`` / pod logs).

    The exception is classified by type name (``SERVER_ERROR_TYPES``) to decide
    what state marker to stamp alongside the sidecar:

    - A transient/server error always gets a retryable
      ``{root_id}.blocked.server_error`` marker, regardless of ``stage``.
    - Otherwise, if ``permanent_if_data`` (set only by the per-cell processing
      stage, where a non-transient failure is a deterministic property of this
      neuron), a permanent ``{root_id}.err.processing`` marker so reruns stop
      re-enqueueing it. Left unmarked (retryable via the sidecar alone) for
      whole-batch load failures and uploads, where an unknown error is more
      likely systemic/infra than a property of one neuron.
    """
    tb = traceback.format_exc()
    print(
        f"[{root_id}] {stage} failed: {type(exc).__name__}: {exc}\n{tb}",
        file=sys.stderr,
    )
    if type(exc).__name__ in SERVER_ERROR_TYPES:
        cf.put(f"{root_id}.{SERVER_ERROR_EXTENSION}", b"")
    elif permanent_if_data:
        cf.put(f"{root_id}.{PROCESSING_ERROR_EXTENSION}", b"")
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


def skeleton_not_exist_ids(cloudpath) -> list[int]:
    """Root ids a worker skipped because their skeleton did not exist yet.

    These carry a retryable ``{root_id}.blocked.skeleton_not_exist`` marker (see
    ``_run_task``) and are exactly the ids to re-submit to the CAVE skeleton
    service — so ``generate_skeletons`` can rebuild a targeted generation call
    from a run's leftovers rather than re-enumerating the whole feather. Ids that
    have since resolved (a ``.osy`` output or a permanent ``.err`` marker) are
    excluded, so a stale marker left behind by a now-succeeded cell is not
    re-requested. Returned sorted for a stable, readable ordering.
    """
    cf = CloudFiles(cloudpath)
    marked: set[int] = set()
    resolved: set[int] = set()
    for f in cf.list():
        kind, reason = marker_parts(f)
        if kind == BLOCKED_PREFIX and reason == "skeleton_not_exist":
            marked.add(int(f.split(".")[0]))
        elif is_resolved(f):
            resolved.add(int(f.split(".")[0]))
    return sorted(marked - resolved)


def _is_pending(cf, root_id: int) -> bool:
    """Whether ``root_id`` still needs processing in this output dir.

    A secondary, per-worker idempotency guard on top of ``filter_tasks`` (which
    dedups at insert time): if a batch is re-leased after a partial run, this
    skips the cells that already resolved. Resolved is the single ``is_resolved``
    notion — a ``.osy`` output or any permanent ``.err.<reason>`` marker — so this
    tracks new err reasons automatically and never diverges from ``filter_tasks``.
    A retryable ``.blocked.<reason>`` marker is not resolved, so it stays pending.

    One prefix-scoped listing (root_ids have no dots, so the trailing ``.`` can't
    match a longer id) rather than a HEAD per candidate marker.
    """
    return not any(is_resolved(f) for f in cf.list(prefix=f"{root_id}."))


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
    markers for permanent data-caused ones, ``.blocked.<reason>`` markers for
    retryable blocks like a not-yet-generated skeleton) rather than raised, so a
    single bad neuron — or a root that is invalid / not-yet-skeletonized — does
    not kill the batch or the worker. Absence of ``{root_id}.osy`` (and of any
    ``.err`` marker) is the signal that a neuron still needs processing (a
    ``.blocked`` marker does not resolve it — see ``filter_tasks``).
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
        # treated as retryable for every pending root, so a later insert_tasks
        # re-enqueues them: a diagnostic sidecar each, plus a .blocked.server_error
        # marker when the error is transient/server-typed. Never permanent here —
        # a batch-level failure is usually systemic, not one neuron's data, so we
        # do not poison the whole batch with .err markers.
        for rid in pending:
            _record_error(cf, f"{rid}.{OSSIFY_EXTENSION}", "skeletonization", rid, e)
        return

    # Roots the batch loader dropped (absent from the result). Classify them with
    # the same shared timestamp: not-latest is a permanent, data-caused failure
    # (mark it so reruns skip it); latest-but-missing means the skeleton has not
    # been generated yet, so we write a retryable .blocked.skeleton_not_exist
    # marker — it does not resolve the id (a rerun retries once the
    # generate_skeletons pre-pass catches up), but it records the id so a targeted
    # generate_skeletons call can be rebuilt from these (see skeleton_not_exist_ids).
    missing = [rid for rid in pending if rid not in cells]
    if missing:
        valid_mask = client.chunkedgraph.is_latest_roots(missing, timestamp=ts)
        for rid, is_valid in zip(missing, valid_mask):
            if not is_valid:
                cf.put(f"{rid}.{INVALID_EXTENSION}", b"")
            else:
                cf.put(f"{rid}.{SKELETON_NOT_EXIST_EXTENSION}", b"")
                print(
                    f"[{rid}] skeleton not generated yet (run generate_skeletons "
                    "and wait); marked .blocked.skeleton_not_exist for retry.",
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
            # A non-transient failure here is a deterministic property of this
            # neuron (bad geometry, a feature that can't be computed): mark it
            # permanent so reruns stop re-enqueueing it. A transient/server error
            # still falls through to a retryable .blocked.server_error marker.
            _record_error(
                cf, filename, "skeletonization", rid, e, permanent_if_data=True
            )
            continue

        try:
            upload_cell(filename, cell, cloudpath)
        except Exception as e:
            # Uploads fail on infra/config far more than on neuron data, so keep
            # them retryable (sidecar, or .blocked.server_error if transient).
            _record_error(cf, filename, "upload", rid, e)


def run_task(*args, **kwargs):
    """You can leave this function alone.

    Returns a partial that the queue producer enqueues; the worker in ``run.py``
    later calls it. This indirection keeps task-queue details out of the real
    work in ``processing.py``.
    """
    return partial(_run_task, *args, **kwargs)
