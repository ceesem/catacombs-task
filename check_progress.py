"""Report processing progress for a config profile.

Compares the profile's task feather (every root_id) against what's landed in
``CF_CLOUDPATH`` and reports how many ids are *resolved* versus outstanding.

An id is **processed** (done — the outcome won't change on retry) when it has
either:

  - a ``.osy`` output (assembled successfully), or
  - a *permanent* ``{root_id}.err.<reason>`` marker or a *data-caused*
    ``*_error.json`` sidecar — a failure that is a property of the neuron itself.

An id is **outstanding** when it carries a *retryable* ``.blocked.<reason>``
marker — ``.blocked.skeleton_not_exist`` (needs a generate_skeletons pass) or
``.blocked.server_error`` (a transient/config error the workers retry) — or
hasn't been attempted yet. This is the same "done" notion as ``task.is_resolved``
/ ``filter_tasks``, extended to fold in data-caused error sidecars and to break
the counts down by cause.

The data-vs-blocked split is the point: a data failure is real progress; a
blocked id is not (it will be retried). Workers stamp transient failures with a
``.blocked.server_error`` marker, so most classification is from the object
names alone; only unmarked ``*_error.json`` sidecars are fetched, to tell a
data-caused failure (permanent) from a stray server one (folded into blocked).

Run it with a profile:  poe check_progress -e <profile>
"""

import argparse
import json
import os
from datetime import datetime, timedelta

import pandas as pd
from cloudfiles import CloudFiles

from catacombs_task.paths import task_feather_path
from catacombs_task.task import (
    BLOCKED_PREFIX,
    ERR_PREFIX,
    OSSIFY_EXTENSION,
    SERVER_ERROR_TYPES,
    marker_parts,
)

# One-line hint printed next to each outstanding .blocked.<reason> count.
BLOCKED_HINTS = {
    "skeleton_not_exist": "run generate_skeletons --from-markers",
    "server_error": "transient/server — will retry",
}


# Breadcrumb the report leaves in the cloudpath so the next run can compute a
# rate/ETA. Named so it is ignored by the listing here and by task.is_resolved
# (not a .osy / .err.<reason> / *_error.json artifact, so it never miscounts).
CHECKPOINT_FILE = "_check_progress_state.json"


def _root_id(filename: str) -> int:
    # Every artifact is "<root_id>.<...>"; root_ids contain no dots.
    return int(filename.split(".")[0])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", help="Config profile under config/ (e.g. v1dd, microns)"
    )
    profile = parser.parse_args().profile

    cloudpath = os.environ.get("CF_CLOUDPATH")
    if cloudpath is None:
        raise ValueError("Must set CF_CLOUDPATH")

    all_ids = set(
        pd.read_feather(task_feather_path(profile))["root_id"].astype("int64").tolist()
    )
    total = len(all_ids)

    # One pass over the output listing: bucket artifacts by kind from the name
    # alone (no fetch needed for .osy / .err.<reason> / .blocked.<reason> markers).
    # err markers are permanent (data-caused, count as processed); blocked markers
    # are retryable (outstanding — see task.is_resolved).
    cf = CloudFiles(cloudpath)
    osy_ids: set[int] = set()
    err_ids_by_reason: dict[str, set[int]] = {}
    blocked_ids_by_reason: dict[str, set[int]] = {}
    error_files: list[str] = []
    for f in cf.list():
        if f.endswith(f".{OSSIFY_EXTENSION}"):
            osy_ids.add(_root_id(f))
            continue
        kind, reason = marker_parts(f)
        if kind == ERR_PREFIX:
            err_ids_by_reason.setdefault(reason, set()).add(_root_id(f))
        elif kind == BLOCKED_PREFIX:
            blocked_ids_by_reason.setdefault(reason, set()).add(_root_id(f))
        elif f.endswith("error.json"):
            error_files.append(f)

    perm_marker_ids: set[int] = (
        set().union(*err_ids_by_reason.values()) if err_ids_by_reason else set()
    )
    blocked_ids: set[int] = (
        set().union(*blocked_ids_by_reason.values()) if blocked_ids_by_reason else set()
    )

    # Classify error sidecars by fetching their JSON — only for ids not already
    # accounted for by a .osy or an .err/.blocked marker. This is the only step
    # that reads object contents. Workers now write a .blocked.server_error marker
    # for transient failures, so the classified sidecars are usually all
    # data-caused; a server-typed sidecar with no marker (e.g. from an older run)
    # is folded into the blocked "server_error" bucket for a single coherent count.
    to_classify = [
        f
        for f in error_files
        if _root_id(f) not in osy_ids
        and _root_id(f) not in perm_marker_ids
        and _root_id(f) not in blocked_ids
    ]
    errjson_data_ids: set[int] = set()
    server_err_ids: set[int] = set()
    for item in cf.get(to_classify) if to_classify else []:
        fid = _root_id(item["path"])
        etype = None
        content = item.get("content")
        if content:
            try:
                etype = json.loads(content).get("error_type")
            except Exception:
                etype = None
        # Unknown/unparseable defaults to data-caused; the server set is the
        # well-defined exclusion.
        target = server_err_ids if etype in SERVER_ERROR_TYPES else errjson_data_ids
        target.add(fid)
    if server_err_ids:
        blocked_ids_by_reason.setdefault("server_error", set()).update(server_err_ids)
        blocked_ids |= server_err_ids

    # Precedence: success (.osy) > data-permanent (.err marker or data sidecar) >
    # blocked (.blocked.<reason>, retryable). Everything intersected with the
    # actual task list.
    success = osy_ids & all_ids
    perm_marker_ids = (perm_marker_ids & all_ids) - osy_ids
    errjson_data_ids = (errjson_data_ids & all_ids) - osy_ids - perm_marker_ids
    data_perm = perm_marker_ids | errjson_data_ids
    blocked_ids = (blocked_ids & all_ids) - osy_ids - data_perm
    not_attempted = total - len(success | data_perm | blocked_ids)

    processed = len(success) + len(data_perm)
    outstanding = total - processed
    pct = (100.0 * processed / total) if total else 0.0

    # Rate/ETA breadcrumb: compare against the checkpoint the previous run left
    # in the cloudpath, then refresh it. The first run has no baseline.
    now_dt = datetime.now().astimezone()
    prev = cf.get_json(CHECKPOINT_FILE) if cf.exists(CHECKPOINT_FILE) else None

    print(f"── progress: {profile} ───────────────────────────────")
    print(f"  as of            : {now_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  cloudpath        : {cloudpath}")
    # Counts are per cell (one .osy per root_id), not per queued task — a task
    # batches TASK_BATCH_SIZE cells, so the queue holds ~total/TASK_BATCH_SIZE tasks.
    print(f"  total cells      : {total}")
    print(f"  processed (done) : {processed}  ({pct:.1f}%)")
    print(f"      succeeded    : {len(success)}   (.osy)")
    print(f"      data errors  : {len(data_perm)}")
    for reason in sorted(err_ids_by_reason):
        n = len((err_ids_by_reason[reason] & all_ids) - osy_ids)
        print(f"          .err.{reason:<12}: {n}")
    print(
        f"          error sidecar : {len(errjson_data_ids)}   (data-caused *_error.json)"
    )
    print(f"  outstanding      : {outstanding}")
    for reason in sorted(blocked_ids_by_reason):
        n = len((blocked_ids_by_reason[reason] & all_ids) - osy_ids - data_perm)
        hint = BLOCKED_HINTS.get(reason, "retryable")
        print(f"      .blocked.{reason:<18}: {n}   ({hint})")
    print(f"      not attempted: {not_attempted}")

    if prev and prev.get("epoch") and now_dt.timestamp() > prev["epoch"]:
        dt_hr = (now_dt.timestamp() - prev["epoch"]) / 3600.0
        d_proc = processed - prev.get("processed", processed)
        rate = d_proc / dt_hr if dt_hr > 0 else 0.0
        print(f"  since last check : +{d_proc} in {dt_hr:.1f} h  ({rate:,.0f}/h)")
        if rate > 0 and outstanding > 0:
            eta_hr = outstanding / rate
            eta = f"{eta_hr:.1f} h" if eta_hr < 48 else f"{eta_hr / 24:.1f} days"
            finish = (now_dt + timedelta(hours=eta_hr)).strftime("%Y-%m-%d %H:%M %Z")
            print(f"  est. remaining   : ~{eta}  (≈ {finish})")
        elif outstanding > 0:
            print("  est. remaining   : n/a (no measurable progress since last check)")
    else:
        print("  since last check : (no prior sample — run again later for a rate)")
    print("───────────────────────────────────────────────────────")

    # Refresh the breadcrumb for the next run.
    cf.put_json(
        CHECKPOINT_FILE,
        {
            "epoch": now_dt.timestamp(),
            "iso": now_dt.isoformat(),
            "processed": processed,
            "total": total,
        },
    )


if __name__ == "__main__":
    main()
