"""Delete stale ``.blocked.<reason>`` markers from a profile's output dir.

A ``.blocked.<reason>`` marker (see ``task.py``) records a *retryable* block —
a not-yet-generated skeleton (``.blocked.skeleton_not_exist``) or a transient
server error (``.blocked.server_error``). Once the block clears and the cell
resolves (a ``.osy`` output, or a permanent ``.err.<reason>`` marker), the
blocked marker is stale: everything still reads correctly because ``.osy`` and
``.err`` take precedence in ``is_resolved`` / the listers, but the marker lingers
in the output dir as cruft. This driver sweeps those stale markers out.

It is safe to run any time: it only deletes ``.blocked.*`` markers for root_ids
that have *already resolved*, so it never removes a marker that still represents
an outstanding block. Outputs (``.osy``), permanent ``.err`` markers, and the
diagnostic ``*_error.json`` sidecars are never touched.

    poe check_progress   -e <profile>   # see how many .blocked markers are stale
    poe cleanup_markers   -e <profile>   # THIS: delete the stale ones

Reads ``CF_CLOUDPATH`` from the same layered env as the other tasks.

Run it with a profile:  ``poe cleanup_markers -e <profile>``.
"""

import argparse
import os
import sys

from cloudfiles import CloudFiles

from catacombs_task.task import BLOCKED_PREFIX, is_resolved, marker_parts


def _root_id(filename: str) -> int:
    # Every artifact is "<root_id>.<...>"; root_ids contain no dots.
    return int(filename.split(".")[0])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", help="Config profile under config/ (e.g. v1dd, microns)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without deleting anything.",
    )
    parser.add_argument(
        "--no-ask",
        action="store_true",
        help="Skip the confirmation prompt and delete immediately (for scripting). "
        "Via poe: `poe cleanup_markers -e <profile> -- --no-ask`.",
    )
    args = parser.parse_args()
    profile = args.profile

    cloudpath = os.environ.get("CF_CLOUDPATH")
    if cloudpath is None:
        raise ValueError("Must set CF_CLOUDPATH")

    # One listing pass: collect resolved root_ids and every .blocked marker,
    # keyed by root_id, from the object names alone (no content fetch).
    cf = CloudFiles(cloudpath)
    resolved_ids: set[int] = set()
    blocked_by_id: dict[int, list[str]] = {}
    for f in cf.list():
        kind, _ = marker_parts(f)
        if kind == BLOCKED_PREFIX:
            blocked_by_id.setdefault(_root_id(f), []).append(f)
        elif is_resolved(f):
            resolved_ids.add(_root_id(f))

    # Stale = a blocked marker whose root_id has since resolved.
    stale = sorted(
        (fn for rid, fns in blocked_by_id.items() if rid in resolved_ids for fn in fns)
    )
    total_blocked = sum(len(fns) for fns in blocked_by_id.values())

    print("── cleanup_markers ───────────────────────────────────")
    print(f"  profile        : {profile}")
    print(f"  cloudpath      : {cloudpath}")
    print(f"  .blocked total : {total_blocked}")
    print(
        f"  stale (resolved): {len(stale)}  (across {len(set(map(_root_id, stale)))} cells)"
    )
    print("───────────────────────────────────────────────────────")

    if not stale:
        print("Nothing to clean up.")
        return

    if args.dry_run:
        for fn in stale:
            print(f"  would delete: {fn}")
        print(f"(dry run — {len(stale)} marker(s) left in place)")
        return

    # Confirm before deleting. --no-ask bypasses for scripted runs; a
    # non-interactive shell without it refuses rather than block on input().
    if not args.no_ask:
        if not sys.stdin.isatty():
            print(
                "Non-interactive shell: re-run with --no-ask to delete without "
                "confirmation (or --dry-run to preview)."
            )
            return
        if input(
            f"Delete {len(stale)} stale marker(s)? [y/N] "
        ).strip().lower() not in (
            "y",
            "yes",
        ):
            print("Aborted; nothing deleted.")
            return

    cf.delete(stale)
    print(f"Deleted {len(stale)} stale .blocked marker(s).")


if __name__ == "__main__":
    main()
