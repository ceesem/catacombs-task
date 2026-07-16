"""Pre-generate skeletons server-side for a config profile.

The ``ossify`` batch loader used by the workers (``load_cell_batch_from_client``
in ``processing.py``) only *downloads* skeletons the CAVE skeleton service has
already produced — it does not generate them on demand. This driver is the
generation pre-pass: it enumerates the run's ``root_ids`` from the task feather
and fires ``client.skeleton.generate_bulk_skeletons_async(...)`` so the service
starts building them, then returns immediately. Generation continues on the
backend and can take a while, so this is its **own command** — run it, wait for
the backend to catch up, then ``insert_tasks`` / launch workers.

    poe build_tasks       -e <profile>   # 1. enumerate the cells
    poe generate_skeletons -e <profile>  # 2. THIS: kick off server-side generation, then wait
    poe insert_tasks      -e <profile>   # 3. seed the queue (batches of TASK_BATCH_SIZE)
    poe launch_worker     -e <profile>   # 4. workers download + assemble

Configuration comes from the same layered env as the other tasks
(``config/common.env`` + ``config/<profile>/task.env``); the datastack, server,
and skeleton version are read straight from the feather so they can never
disagree with what ``build_tasks.py`` wrote. If ``CF_CLOUDPATH`` is set, cells
already assembled there are skipped (via ``filter_tasks``), so a re-run only
generates what is still outstanding.

Run it with a profile:  ``poe generate_skeletons -e <profile>``.
"""

import argparse
import os
import sys
from time import sleep

import pandas as pd
from caveclient import CAVEclient

from catacombs_task.paths import task_feather_path
from catacombs_task.task import filter_tasks

# CAVE's generate_bulk_skeletons_async processes at most this many root_ids per
# call (caveclient.skeletonservice.MAX_BULK_ASYNCHRONOUS_SKELETONS); larger runs
# are submitted in successive chunks.
BULK_ASYNC_LIMIT = 10_000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", help="Config profile under config/ (e.g. v1dd, microns)"
    )
    parser.add_argument(
        "--no-ask",
        action="store_true",
        help="Skip the confirmation prompt and submit immediately (for scripting). "
        "Via poe: `poe generate_skeletons -e <profile> -- --no-ask`.",
    )
    args = parser.parse_args()
    profile = args.profile

    parameter_filename = task_feather_path(profile)
    task_df = pd.read_feather(parameter_filename)

    # Skip cells already assembled in the output dir, if one is configured, so a
    # re-run only regenerates what is still outstanding (generation is otherwise
    # idempotent, just wasteful).
    original_number = len(task_df)
    target_cloudpath = os.environ.get("CF_CLOUDPATH")
    if target_cloudpath:
        task_df = filter_tasks(task_df, target_cloudpath)

    if len(task_df) == 0:
        print("No skeletons to generate!")
        return

    datastack = task_df["datastack_name"].iloc[0]
    server = task_df["server_address"].iloc[0]
    skeleton_version = int(task_df["skeleton_version"].iloc[0])
    materialization_version = (
        int(task_df["materialization_version"].iloc[0])
        if "materialization_version" in task_df.columns
        else None
    )
    root_ids = task_df["root_id"].tolist()
    n_chunks = -(-len(root_ids) // BULK_ASYNC_LIMIT)  # ceil division

    print("── generate_skeletons ────────────────────────────────")
    print(f"  profile        : {profile}")
    print(f"  datastack      : {datastack}")
    print(f"  skeleton ver   : {skeleton_version}")
    print(f"  feather        : {parameter_filename}")
    print(
        f"  root_ids       : {len(root_ids)} to generate "
        f"({original_number - len(root_ids)} already assembled, skipped)"
    )
    print(f"  async requests : {n_chunks} chunk(s) of ≤{BULK_ASYNC_LIMIT}")
    print("───────────────────────────────────────────────────────")

    # Confirm before kicking off backend generation — this can enqueue a large,
    # long-running server-side job. --no-ask bypasses for scripted runs; a
    # non-interactive shell without it refuses rather than block on input().
    if not args.no_ask:
        if not sys.stdin.isatty():
            print(
                "Non-interactive shell: re-run with --no-ask to submit without "
                "confirmation."
            )
            return
        if input("Submit generation? [y/N] ").strip().lower() not in ("y", "yes"):
            print("Aborted; nothing submitted.")
            return

    client = CAVEclient(
        datastack, server_address=server, version=materialization_version
    )

    total_estimate = 0.0
    for i in range(0, len(root_ids), BULK_ASYNC_LIMIT):
        chunk = root_ids[i : i + BULK_ASYNC_LIMIT]
        result = client.skeleton.generate_bulk_skeletons_async(
            chunk,
            datastack_name=datastack,
            skeleton_version=skeleton_version,
        )
        # The async call returns an estimated time in seconds (older servers may
        # return a non-numeric payload); accumulate it when it is a number.
        if isinstance(result, (int, float)):
            total_estimate += float(result)
        sleep(30)
    print(
        f"Submitted {len(root_ids)} root_ids for generation "
        f"(estimated ~{total_estimate / 60:.1f} min of backend work)."
    )
    print(f"Wait for the backend to finish, then:  poe insert_tasks -e {profile}")


if __name__ == "__main__":
    main()
