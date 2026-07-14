# catacombs-task

Assemble ossify skeletons for a dataset

## Setup

This project uses `uv` for dependency management and `poe` for task running.

```bash
# Install dependencies (creates the virtual environment)
uv sync
```


## Task Queue Setup

Workers pull one-neuron tasks from a queue; a config **profile** captures
everything specific to a run. Each `poe` task takes a required `-e/--env`
selector naming the profile.

1. Implement your task in `src/catacombs_task/task.py` (plumbing) and `src/catacombs_task/processing.py` (the work)
2. Create a profile under `config/<profile>/`: `task.env` + `cluster.env` + `build.py` (a `get_root_ids(client)` recipe), layered on `config/common.env` — see `config/README.md`
3. Build the task feather: `poe build_tasks -e <profile>`
4. Insert the tasks into the queue: `poe insert_tasks -e <profile>`
5. Run workers locally (`poe launch_worker -e <profile>`) or deploy to GKE (`poe deploy_task -e <profile>`)

See `HOW_TO_USE_TASKS.md` for the full walkthrough.

## License

MIT License - see LICENSE file for details.
