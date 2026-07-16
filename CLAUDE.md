# catacombs_task

Assemble ossify skeletons for a dataset

## Team Tools & Domain Context

<!-- See team reference doc for CAVE ecosystem, caveclient, neuroglancer,
     task queue patterns, and connectomics-specific design decisions:
     [link TBD] -->

## Development Environment

```bash
# Install dependencies (creates virtual environment)
uv sync

# Add a new dependency
uv add <package>

# Linting / formatting
uv run ruff check src/
uv run ruff format src/
```

### Key Commands

Every task/cluster command takes a **required** `-e/--env <profile>` selector
(e.g. `-e v1dd`, `-e microns`) — see `config/README.md`.

| Command | Description |
|---------|-------------|
| `poe build_tasks -e <profile>` | Build the task feather for a profile |
| `poe generate_skeletons -e <profile>` | Pre-generate skeletons server-side (run after build, before insert) |
| `poe insert_tasks -e <profile>` | Insert tasks into the task queue (batches of `TASK_BATCH_SIZE` cells) |
| `poe launch_worker -e <profile>` | Launch a local worker process |
| `poe launch_worker_docker -e <profile>` | Run a worker in the Docker image |
| `poe check_filequeue -e <profile>` | Check filequeue status |
| `poe cleanup_markers -e <profile>` | Delete stale `.blocked.*` markers for cells that have since resolved (`-- --dry-run` to preview) |
| `poe reset_queue -e <profile>` | Reset the file queue |
| `poe make_scripts -e <profile>` | Render Kubernetes scripts (optional; preview only — the cluster tasks auto-render) |
| `poe launch_cluster -e <profile>` | Render scripts + create the GKE cluster (idempotent: if it already exists, resizes the node pool to `NUMBER_NODES`) |
| `poe use_context -e <profile>` | Point kubectl at that profile's cluster (switch/refresh) |
| `poe apply_task -e <profile>` | Render scripts + set context + deploy the task |
| `poe deploy_task -e <profile>` | Create the cluster and deploy the task |
| `poe delete_cluster -e <profile>` | Delete the cluster for a profile (all its resources) |

## Task Queue Workflow

Workers pull tasks from a queue (filequeue for local dev, cloud queue for production).

1. Implement your task logic in `src/catacombs_task/task.py`
2. Configure a profile under `config/<profile>/` (`task.env` + `cluster.env` + `build.py`), layered on `config/common.env` — see `config/README.md`
3. Build the task feather: `poe build_tasks -e <profile>`
4. Pre-generate skeletons: `poe generate_skeletons -e <profile>`, then **wait** for the CAVE skeleton service to finish. Workers only download skeletons (via ossify's batch loader); they do not generate them on demand.
5. Insert tasks: `poe insert_tasks -e <profile>` — cells are grouped into batches of `TASK_BATCH_SIZE` (default 10) per queued task; each worker loads a whole batch in one pooled ossify call.

### Kubernetes Deployment

```bash
# Step-by-step (each command re-renders scripts from the profile first)
poe launch_cluster -e <profile>   # render + create the GKE cluster
poe apply_task -e <profile>       # render + deploy the task to the cluster

# Or all-in-one
poe deploy_task -e <profile>

# Preview the rendered scripts without launching (optional)
poe make_scripts -e <profile>

# Cleanup
poe delete_cluster -e <profile>
```

See `HOW_TO_USE_TASKS.md` for detailed setup instructions.

---

## Project Overview

<!-- Describe what this project does and why it exists -->

## Architecture & Key Files

<!-- List key files and how they fit together -->

## Data & External Dependencies

<!-- Document datasets, services, APIs, and file paths this project depends on -->

## Conventions & Patterns

<!-- Describe coding conventions, naming patterns, and design decisions -->

## Notes

<!-- Miscellaneous notes, gotchas, TODOs -->
