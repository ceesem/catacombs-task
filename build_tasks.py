"""Build a task-parameter feather for a config profile.

This is the *dynamic task* side of the pipeline: it enumerates the units of
work (one row per ``root_id``) and stamps the per-task parameters. Each column
becomes a keyword argument to ``_run_task`` in ``src/catacombs_task/task.py``:

    root_id                 (int)       required   the neuron to assemble
    datastack_name          (str)       required   CAVE datastack        (DATASTACK_NAME)
    server_address          (str)       required   CAVE global server    (SERVER_ADDRESS)
    restore_properties      (bool)      optional   full L2-cache props   (RESTORE_PROPERTIES)
    skeleton_version        (int)       optional   skeleton-service ver  (SKELETON_VERSION)
    materialization_version (int)       optional   pinned CAVE mat. ver  (client.version set in build.py)
    reference_tables        (list[str]) optional   synapse ref tables    (REFERENCE_TABLES_MAP keys)
    reference_suffixes      (dict)      optional   ref-table col suffixes (REFERENCE_TABLES_MAP)
    compartment_model       (str)       optional   axon/dendrite model   (COMPARTMENT_MODEL)

The two optional columns are only stamped when their profile value is set;
otherwise ``_run_task`` uses its own defaults (no reference tables, and the
compartment step is skipped).

This is a single, profile-agnostic driver — it holds no dataset specifics. Those
come from two places in the selected profile, so there is exactly one source of
truth per dataset:

  - config/<profile>/task.env   the values (datastack, server, params, …),
                                loaded into the environment by the poe task.
  - config/<profile>/build.py   the only per-dataset *code*: a
                                ``get_root_ids(client) -> list[int]`` function.

The feather is written to the path ``task_feather_path(profile)`` derives from
the profile name (``config/<profile>/tasks/<profile>.feather`` by default) — the
same path ``insert_tasks.py`` reads — so building and inserting never disagree,
and no path has to be spelled out per profile. Set ``TASK_PARAMETER_FILENAME`` in
task.env only to override that default.

Run it with a profile:  ``poe build_tasks -e <profile>``.
"""

import argparse
import importlib.util
import os
from pathlib import Path

import pandas as pd
from caveclient import CAVEclient

from catacombs_task.paths import task_feather_path

CONFIG_ROOT = Path(__file__).parent / "config"


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Must set {name} (in config/<profile>/task.env)")
    return value


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _env_dict(name: str) -> dict[str, str]:
    """Parse comma-separated ``key:value`` pairs into a dict (empty if unset).

    A bare ``key`` with no ``:`` maps to itself, e.g.
    ``REFERENCE_TABLES_MAP=table_a:a,table_b:b``.
    """
    out: dict[str, str] = {}
    for item in os.environ.get(name, "").split(","):
        item = item.strip()
        if not item:
            continue
        key, _, value = item.partition(":")
        key = key.strip()
        out[key] = value.strip() or key
    return out


def load_recipe(profile: str):
    """Import ``config/<profile>/build.py`` and return its module.

    The module must define ``get_root_ids(client) -> list[int]`` — the one piece
    of code that is genuinely unique per dataset.
    """
    path = CONFIG_ROOT / profile / "build.py"
    if not path.exists():
        raise FileNotFoundError(
            f"No build recipe at {path}. Create it with a "
            "get_root_ids(client) -> list[int] function "
            "(see config/examples/build.py.example)."
        )
    spec = importlib.util.spec_from_file_location(f"catacombs_recipe_{profile}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "get_root_ids"):
        raise AttributeError(f"{path} must define get_root_ids(client) -> list[int]")
    return module


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "profile", help="Config profile under config/ (e.g. v1dd, microns)"
    )
    profile = parser.parse_args().profile

    datastack = _require("DATASTACK_NAME")
    server = _require("SERVER_ADDRESS")
    out = task_feather_path(profile)
    skeleton_version = int(os.environ.get("SKELETON_VERSION", "4"))
    restore_properties = _env_bool("RESTORE_PROPERTIES", True)
    # Constant-across-run knobs. All optional: left unset here, the column is
    # omitted and _run_task falls back to its own default.
    # REFERENCE_TABLES_MAP is one source for both ossify inputs: its keys are the
    # reference tables to attach, its values the per-table column suffix.
    reference_tables_map = _env_dict("REFERENCE_TABLES_MAP")
    compartment_model = os.environ.get("COMPARTMENT_MODEL", "").strip()

    recipe = load_recipe(profile)
    client = CAVEclient(datastack, server_address=server)
    root_ids = recipe.get_root_ids(client)

    # Capture the materialization version the recipe pinned (recipes set
    # ``client.version`` in get_root_ids — see config/<profile>/build.py). Stamping
    # it into the feather locks the query timestamp for the whole run: the ids were
    # enumerated at this snapshot, and every batch a worker later loads shares the
    # same timestamp. None (recipe left it unpinned) falls back to latest.
    materialization_version = client.version

    task_df = pd.DataFrame({"root_id": pd.array(root_ids, dtype="int64")})
    task_df["datastack_name"] = datastack
    task_df["server_address"] = server
    task_df["restore_properties"] = restore_properties
    task_df["skeleton_version"] = skeleton_version
    if materialization_version is not None:
        task_df["materialization_version"] = int(materialization_version)
    if reference_tables_map:
        # Same values on every row (constant across the run); _run_task coerces
        # them back to a plain list / dict.
        task_df["reference_tables"] = [list(reference_tables_map)] * len(task_df)
        task_df["reference_suffixes"] = [reference_tables_map] * len(task_df)
    if compartment_model:
        task_df["compartment_model"] = compartment_model

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    task_df.to_feather(out)
    # One row per cell (root_id). insert_tasks later groups these into queued
    # tasks of TASK_BATCH_SIZE cells each — so the queue holds far fewer tasks
    # than there are rows here.
    print(f"Wrote {len(task_df)} cells (one row per root_id) to {out}")
    print(f"Insert them with:  poe insert_tasks -e {profile}")


if __name__ == "__main__":
    main()
