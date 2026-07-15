# Config profiles

Similar runs against **different servers/datasets** (e.g. V1DD vs MICrONS) are
organized as *profiles*. A profile is a self-contained directory under `config/`
holding its env files, its root-id selection code, and its own task feathers.
Shared defaults live in `config/common.env` and are layered underneath every
profile.

```
config/
  common.env          # shared defaults, loaded FIRST (infra, worker image, resources)
  v1dd/
    task.env          # which data: queue / feather path / cloudpath / datastack / servers
    cluster.env       # cluster identity (TASK_NAME, GOOGLE_CLUSTER_NAME)
    build.py          # get_root_ids(client) — the only per-dataset code
    tasks/            # task feathers built for this profile (git-ignored)
  microns/
    task.env
    cluster.env
    build.py
    tasks/
```

## Selecting a profile

Every `poe` task takes a single, **required** `-e/--env <name>` selector. There
is no default — you always name the profile:

```bash
poe insert_tasks  -e v1dd        # seed the V1DD queue
poe launch_worker -e v1dd        # run a local V1DD worker
poe deploy_task   -e microns     # render scripts, launch cluster, deploy MICrONS
```

`<name>` is the profile directory name (`v1dd`, `microns`, …).

## How the layering works

Files are loaded in this order, and **later files override earlier ones**:

1. `config/common.env`
2. `config/<env>/task.env`
3. `config/<env>/cluster.env` (cluster tasks only)

- Python/queue tasks (`insert_tasks`, `launch_worker`, `check_filequeue`, …)
  load `common.env` + `<env>/task.env` via repeated `uv run --env-file`.
- Cluster tasks (`launch_cluster`, `apply_task`, `deploy_task`, `delete_cluster`,
  and `make_scripts`) additionally source `<env>/cluster.env`. They all render
  first: `scripts/process_template.sh` sources all three files in order and
  renders `templates/* → config/<env>/scripts/*` with `envsubst` before the
  cluster action runs, so there is no separate render step to remember. Each
  profile renders into its own `scripts/` dir, so one profile's rendered
  scripts can never shadow another's.

To override a common value for one dataset, just re-declare it in that
profile's `task.env` or `cluster.env`.

## What goes where

| File | Role | Typical contents |
|------|------|------------------|
| `common.env` | *How to run* — same for every dataset | `TQ_LEASE_SECONDS`, `GOOGLE_PROJECT_NAME`, network/zone/machine type, `NUMBER_NODES`, `PODS_PER_NODE`, `TASK_DOCKER_IMAGE`, `TASK_REPLICAS`, memory/CPU requests & limits |
| `<env>/task.env` | *Which data / servers* | `TASKQUEUE_PATH`, `CF_CLOUDPATH`, `DATASTACK_NAME`, `SERVER_ADDRESS`, `SKELETON_VERSION`, `RESTORE_PROPERTIES`, `REFERENCE_TABLES_MAP`, `COMPARTMENT_MODEL`, `CAVE_GLOBAL_SERVER`, `CAVE_LOCAL_SERVER` (+ optional `TASK_PARAMETER_FILENAME` override) |
| `<env>/cluster.env` | *Cluster identity* | `TASK_NAME`, `GOOGLE_CLUSTER_NAME` (+ any per-dataset overrides) |
| `<env>/build.py` | *How to pick neurons* | a single `get_root_ids(client) -> list[int]` function |

Notes:

- `DATASTACK_NAME` / `SERVER_ADDRESS` (server WITH `https://`) name the CAVE
  datastack and global server. `build_tasks.py` connects to them and stamps
  them into the feather (per-row `datastack_name` / `server_address` columns),
  so the workers pick them up from there. `CAVE_GLOBAL_SERVER` /
  `CAVE_LOCAL_SERVER` (no scheme) serve a *different* purpose — naming the
  `*-cave-secret.json` files baked into the cluster secret by
  `templates/make_cluster.sh`.
- The task feather's path is **derived from the profile name** —
  `config/<env>/tasks/<env>.feather` — by both `build_tasks.py` (writer) and
  `insert_tasks.py` (reader), so there is nothing to spell out per profile and
  the two can never disagree. Build it with `poe build_tasks -e <env>`; the
  feathers are not committed (see `config/<env>/tasks/.gitignore`). Set
  `TASK_PARAMETER_FILENAME` in `task.env` only to override that default location.

## Adding a new profile

```bash
mkdir config/<name>
cp config/microns_v1822/task.env config/microns_v1822/cluster.env config/<name>/
cp config/examples/build.py.example config/<name>/build.py
# edit the copies for the new dataset/server, fill in build.py's get_root_ids, then:
poe build_tasks  -e <name>     # writes config/<name>/tasks/<name>.feather
poe insert_tasks -e <name>
```

Note there is no feather path to fix up in the copied `task.env` — it is derived
from the profile name, so a copied profile writes/reads its *own* `tasks/`
directory automatically.

`config/examples/task.env.example`, `config/examples/cluster.env.example`, and
`config/examples/build.py.example` are references for the two env files and the
build recipe.
