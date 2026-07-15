"""Report processing progress for a config profile.

Compares the profile's task feather (every root_id) against what's landed in
``CF_CLOUDPATH`` and reports how many ids are *resolved* versus outstanding.

An id is **processed** (done — the outcome won't change on retry) when it has
either:

  - a ``.osy`` output (assembled successfully), or
  - any ``{root_id}.err.<reason>`` marker or a *data-caused* ``*_error.json``
    sidecar — a failure that is a property of the neuron itself.

An id is **outstanding** when it hit a *server-caused* error (transient/config,
which the workers retry and which should eventually succeed) or hasn't been
attempted yet. This is the same "done" notion as ``task.is_resolved`` /
``filter_tasks``, extended to fold in data-caused error sidecars and to break
the counts down by cause.

The data-vs-server split is the point: a data failure is real progress; a
server failure is not (it will be retried).

Run it with a profile:  poe check_progress -e <profile>
"""

import argparse
import json
import os
from datetime import datetime, timedelta

import pandas as pd
from cloudfiles import CloudFiles

from catacombs_task.paths import task_feather_path
from catacombs_task.task import ERR_PREFIX, OSSIFY_EXTENSION

# error_type values (from the *_error.json sidecars) that are transient/server-
# or config-caused rather than a property of the neuron's data. These are NOT
# counted as processed: the workers retry them and they should eventually
# succeed. Any error_type NOT listed here is treated as data-caused (permanent)
# — extend this set as new transient types show up in a run.
#
# ServerIncompatibilityError is a caveclient API-version check; it usually rides
# along with a 503 storm (a bad version read during a degraded window) and
# clears on retry — hence "server". If it instead stays stuck and dominates the
# server bucket, that's a real client/server version mismatch to fix, not wait.
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
    # alone (no fetch needed for .osy / .err.<reason> markers).
    cf = CloudFiles(cloudpath)
    osy_ids: set[int] = set()
    err_ids_by_reason: dict[str, set[int]] = {}
    error_files: list[str] = []
    for f in cf.list():
        if f.endswith(f".{OSSIFY_EXTENSION}"):
            osy_ids.add(_root_id(f))
            continue
        parts = f.split(".")
        if len(parts) >= 3 and parts[1] == ERR_PREFIX:  # "<id>.err.<reason>"
            err_ids_by_reason.setdefault(".".join(parts[2:]), set()).add(_root_id(f))
        elif f.endswith("error.json"):
            error_files.append(f)

    marker_ids: set[int] = (
        set().union(*err_ids_by_reason.values()) if err_ids_by_reason else set()
    )

    # Classify error sidecars by fetching their JSON — only for ids not already
    # resolved by a .osy or an .err.<reason> marker. This is the only step that
    # reads object contents.
    to_classify = [
        f
        for f in error_files
        if _root_id(f) not in osy_ids and _root_id(f) not in marker_ids
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

    # Precedence: success (.osy) > data-permanent (marker or data sidecar) >
    # server-retryable. Everything intersected with the actual task list.
    success = osy_ids & all_ids
    marker_ids = (marker_ids & all_ids) - osy_ids
    errjson_data_ids = (errjson_data_ids & all_ids) - osy_ids - marker_ids
    data_perm = marker_ids | errjson_data_ids
    server_pending = (server_err_ids & all_ids) - osy_ids - data_perm
    not_attempted = total - len(success | data_perm | server_pending)

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
    print(f"  total tasks      : {total}")
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
    print(
        f"      server/retry : {len(server_pending)}   (transient/config — will retry)"
    )
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
