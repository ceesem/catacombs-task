"""Summarize the server errors building up under ``.blocked.server_error``.

A ``{root_id}.blocked.server_error`` marker (see ``task.py``) is an empty flag —
the actual detail lives in the ``{root_id}.osy_<stage>_error.json`` sidecar that
``_record_error`` writes alongside it (exception type, message, traceback). This
driver collects the ``.blocked.server_error`` cohort, fetches their sidecars, and
prints a histogram by exception type with an example message and the stage(s) it
hit — so you can tell "1,100 × HTTPError 503 from skeletonization" from a genuine
config problem at a glance.

By default it reports only the *live* backlog (cohort minus ids that have since
resolved via a ``.osy`` output or a permanent ``.err`` marker); pass
``--include-resolved`` to also count stale markers from cells that recovered.

    poe check_progress   -e <profile>   # how many are stuck under .blocked.server_error
    poe inspect_errors   -e <profile>   # THIS: what those errors actually are

Reads ``CF_CLOUDPATH`` from the same layered env as the other tasks.

Run it with a profile:  ``poe inspect_errors -e <profile>``.
"""

import argparse
import json
import os
from collections import Counter

from cloudfiles import CloudFiles

from catacombs_task.task import (
    BLOCKED_PREFIX,
    SERVER_ERROR_EXTENSION,
    is_resolved,
    marker_parts,
)


def _root_id(filename: str) -> int:
    # Every artifact is "<root_id>.<...>"; root_ids contain no dots.
    return int(filename.split(".")[0])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", help="Config profile under config/ (e.g. v1dd, microns)"
    )
    parser.add_argument(
        "--include-resolved",
        action="store_true",
        help="Also count .blocked.server_error markers on cells that have since "
        "resolved (stale markers), not just the live backlog.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2000,
        metavar="N",
        help="Cap how many sidecars to fetch (default 2000; 0 = all). When the "
        "cohort is larger, the counts are from a sample and marked as such.",
    )
    parser.add_argument(
        "--examples",
        type=int,
        default=1,
        metavar="N",
        help="How many distinct example messages to show per error type (default 1).",
    )
    args = parser.parse_args()
    profile = args.profile

    cloudpath = os.environ.get("CF_CLOUDPATH")
    if cloudpath is None:
        raise ValueError("Must set CF_CLOUDPATH")

    # One listing pass: find the .blocked.server_error cohort, which ids have
    # since resolved, and every error sidecar (keyed by root_id) — all from names.
    cf = CloudFiles(cloudpath)
    server_ids: set[int] = set()
    resolved_ids: set[int] = set()
    sidecars_by_id: dict[int, list[str]] = {}
    for f in cf.list():
        kind, reason = marker_parts(f)
        if kind == BLOCKED_PREFIX and reason == "server_error":
            server_ids.add(_root_id(f))
        elif is_resolved(f):
            resolved_ids.add(_root_id(f))
        elif f.endswith("error.json"):
            sidecars_by_id.setdefault(_root_id(f), []).append(f)

    cohort = server_ids if args.include_resolved else server_ids - resolved_ids
    stale = len(server_ids & resolved_ids)

    # Gather the sidecar files for the cohort, sampling up to --limit ids.
    cohort_sorted = sorted(cohort)
    sampled_ids = (
        cohort_sorted
        if args.limit == 0 or len(cohort_sorted) <= args.limit
        else cohort_sorted[: args.limit]
    )
    to_fetch = [fn for rid in sampled_ids for fn in sidecars_by_id.get(rid, [])]
    no_sidecar = sum(1 for rid in sampled_ids if rid not in sidecars_by_id)

    print(f"── server errors: {profile} ──────────────────────────")
    print(f"  cloudpath        : {cloudpath}")
    print(
        f"  .{SERVER_ERROR_EXTENSION} : {len(server_ids)} cells"
        + (f"  ({stale} stale, since resolved)" if stale else "")
    )
    print(
        f"  inspecting       : {len(sampled_ids)} cells"
        + ("" if len(sampled_ids) == len(cohort) else f" of {len(cohort)} (sampled)")
        + (" [live backlog]" if not args.include_resolved else " [incl. resolved]")
    )
    print("───────────────────────────────────────────────────────")

    if not cohort:
        print("No .blocked.server_error cells. Nothing to inspect.")
        return

    # Fetch and tally by exception type. Each sidecar's JSON carries error_type,
    # stage, and the message; group by type, keeping stages and a few examples.
    counts: Counter[str] = Counter()
    stages: dict[str, set[str]] = {}
    examples: dict[str, list[str]] = {}
    unreadable = 0
    for item in cf.get(to_fetch) if to_fetch else []:
        content = item.get("content")
        if not content:
            unreadable += 1
            continue
        try:
            data = json.loads(content)
        except Exception:
            unreadable += 1
            continue
        etype = data.get("error_type") or "unknown"
        counts[etype] += 1
        stages.setdefault(etype, set()).add(data.get("stage") or "?")
        msg = " ".join((data.get("error") or "").split())  # collapse whitespace
        ex = examples.setdefault(etype, [])
        if msg and msg not in ex and len(ex) < args.examples:
            ex.append(msg)

    if not counts:
        print("Cohort has no readable *_error.json sidecars to summarize.")
        if no_sidecar:
            print(f"  ({no_sidecar} cell(s) had no sidecar at all)")
        return

    total = sum(counts.values())
    print(f"  {'error_type':<24} {'count':>7}  {'%':>5}  stage(s)")
    for etype, n in counts.most_common():
        pct = 100.0 * n / total
        print(f"  {etype:<24} {n:>7}  {pct:>4.0f}%  {','.join(sorted(stages[etype]))}")
        for ex in examples.get(etype, []):
            trimmed = ex if len(ex) <= 100 else ex[:97] + "..."
            print(f"      e.g. {trimmed}")
    print("───────────────────────────────────────────────────────")
    print(f"  sidecars summarized : {total}")
    if unreadable:
        print(f"  unreadable sidecars : {unreadable}")
    if no_sidecar:
        print(f"  cells w/o sidecar   : {no_sidecar}")


if __name__ == "__main__":
    main()
