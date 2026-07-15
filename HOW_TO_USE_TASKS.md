# Tasks for catacombs-task

## Task organization

This is a template for running python tasks using [python-task-queue](https://github.com/seung-lab/python-task-queue).
Task-queue is built around splitting up `tasks` that queue up a list of parameters defining work to be done and `workers` that listen to this queue and execute the tasks.
The workers can be run on a local machine, in a docker image, or in a cloud service like Google Cloud Platform.
This package is designed in particular to work with GCP, and has templates and scripts to help you Dockerize your worker and deploy it to a GKE cluster.

### Config profiles

Configuration is organized into **profiles** so you can run the same code
against different servers, datasets, or runs. A profile is a self-contained
directory under `config/` that holds everything specific to one run:

```
config/<profile>/
  task.env      # which data: queue / feather path / cloudpath / datastack / servers / task params
  cluster.env   # cluster identity (TASK_NAME, GOOGLE_CLUSTER_NAME)
  build.py      # get_root_ids(client) — the only per-dataset code
  tasks/        # task feathers built for this profile (git-ignored)
```

Shared defaults live in `config/common.env` and are layered underneath every
profile (later files override earlier ones). Because a profile captures a whole
run's inputs in one git-tracked directory, the convention is to make a **new
profile per run** (e.g. `config/microns_v1822/`) rather than overwriting an
existing one — the run's provenance stays recorded without committing the
(regenerable) feather.

Every `poe` task takes a single, **required** `-e/--env <name>` selector that
names the profile — there is no default:

```bash
poe build_tasks   -e microns_v1822
poe insert_tasks  -e microns_v1822
poe deploy_task   -e microns_v1822
```

See `config/README.md` for the full layout and what belongs in each file.

## Using this template 

The organization of this project is structured in a particular way to work with `python-task-queue` with minimal fuss using [`uv`](https://docs.astral.sh/uv/) and [`poe`](https://poethepoet.natn.io/index.html).

The main components are:

### 1. Tasks and queues

#### Defining the task function

`src/catacombs_task/task.py` is where your task is defined.
The `run_task` function defined here is a simple pass-through that implements the principal mechanism for building up the task queue and otherwise passes all arguments and keywords to the `_run_task` function.
The `_run_task` function is where the actual work of your individual task and its arguments should be defined.
By splitting up the task definition in this way, the details of task-queue can be ignored and `_run_task` can be written as a regular python function, plus the default scripts to populate the queue and run the workers can be used without modification.
In this project `_run_task` keeps only the task-queue plumbing (idempotency check, error sidecars) and delegates the real ossify work to plain functions in `src/catacombs_task/processing.py`, so that logic can be imported, tested, or run in a notebook without touching task-queue.
Each keyword of `_run_task` (`root_id`, `datastack_name`, `server_address`, `restore_properties`, `skeleton_version`, `reference_tables`, `compartment_model`, `cloudpath`) is supplied by a column of the task dataframe.

#### Defining the queue

The config that defines how to populate and run the queue lives in a profile's `config/<profile>/task.env` (layered on `config/common.env`), split into two groups:

- **How to run** — `TASKQUEUE_PATH` (the queue) and `TQ_LEASE_SECONDS` (per-task timeout). Decide on a queue by reading the [python-task-queue](https://github.com/seung-lab/python-task-queue) documentation, then set `TASKQUEUE_PATH` to the path that defines it (e.g. an https URL for an SQS queue or an `fq://` path for a filequeue). Workers look at this queue to find tasks, and a task is reclaimed if it takes longer than `TQ_LEASE_SECONDS`.
- **Which data** — the values `build_tasks.py` stamps onto every row of the task feather: `DATASTACK_NAME`, `SERVER_ADDRESS`, `SKELETON_VERSION`, `RESTORE_PROPERTIES`, and the optional `REFERENCE_TABLES_MAP` (comma-separated `table:suffix` pairs — keys are the reference tables, values their column suffixes) and `COMPARTMENT_MODEL`. Plus `CF_CLOUDPATH` (the output location). The feather's own path is derived from the profile name (see below), so it isn't a value you set. See `config/README.md` for the full table.

To generate the queue you build a pandas dataframe where each row is one task and each column name matches a `_run_task` keyword, then save it as a feather. **`build_tasks.py` does this for you** — you never write the boilerplate. Running `poe build_tasks -e <profile>`:

1. loads `config/common.env` + `config/<profile>/task.env` into the environment;
2. imports the profile's `config/<profile>/build.py` and calls its `get_root_ids(client) -> list[int]` — the *only* per-dataset code you write;
3. stamps the "which data" values above onto one row per returned `root_id`;
4. writes the feather to the path derived from the profile name, `config/<profile>/tasks/<profile>.feather`.

So the per-dataset recipe is tiny — for example, `config/microns_v1822/build.py`:

```python
def get_root_ids(client) -> list[int]:
    client.version = 1822  # pin the materialization
    neuron_df = client.materialize.views.single_neurons.get_all()
    return neuron_df.drop_duplicates("pt_root_id").query("pt_root_id != 0")["pt_root_id"].tolist()
```

Both `build_tasks.py` (writer) and `insert_tasks.py` (reader) derive that path the same way from the profile name — `config/<profile>/tasks/<profile>.feather` — so the two can never disagree and there is no path to spell out or keep in sync (set `TASK_PARAMETER_FILENAME` in `task.env` only to override it). The feathers live under `config/<profile>/tasks/` and are not checked into git (see `config/<profile>/tasks/.gitignore`); regenerate them from the current materialization as needed. Adding a dataset or a new run is therefore just a new `config/<profile>/` directory (`task.env` + `cluster.env` + `build.py`) — copy an existing profile, no path edits, no forking of the build script.

The current template assumes you will use [CloudFiles](https://github.com/seung-lab/cloud-files) to store the processed data from the task queue, and the `CF_CLOUDPATH` environment variable should be set to the path of the CloudFiles directory or cloud bucket where the processed data will be stored.
The script `insert_tasks.py` reads the profile's feather (via `task_feather_path`) and uses `CF_CLOUDPATH` to add a `cloudpath` to each row, assuming `cloudpath` is an argument of your `_run_task` function.
Modify this as needed.

```python
from catacombs_task.paths import task_feather_path

task_df = pd.read_feather(task_feather_path(profile))
target_cloudpath = os.environ.get("CF_CLOUDPATH")

if "cloudpath" not in task_df.columns:
    task_df["cloudpath"] = target_cloudpath
```

#### Inserting Tasks

To insert tasks into the queue, run the `insert_tasks.py` script with the above environment variables set.

The easiest way to do this is to use `poe` to run the `insert_tasks` script in `uv` with an environment file.
Assuming you have already have an established python environment created with `uv sync` or anything else that generates a `uv.lock` file, you can insert tasks via `poe insert_tasks -e <profile>` (e.g. `poe insert_tasks -e v1dd`).
The `-e/--env` flag is required and names a profile directory under `config/`; the task loads `config/common.env` followed by `config/<profile>/task.env`.
Note that this command will expect those files to exist relative to the root of the project directory.
If you don't want that for some reason, run via `uv run --frozen insert_tasks.py`.
Note that the `--frozen` flag is important to make sure that the tasks and the workers are running the same code.

##### Skipping finished tasks

Sometimes tasks will fail, and most likely your `_run_task` function will handle it via `try/except` blocks instead of raising an Exception that will kill a worker.
Because of this, after an initial run-through, you will often want to start from the same initial state and only add tasks that have not been completed previously.
The `filter_tasks` function in `src/catacombs_task/task.py` handles this: `insert_tasks.py` calls it with the task dataframe and `CF_CLOUDPATH`, and it returns only the tasks whose output is not already present.

As shipped, a completed neuron is signalled by a `{root_id}.osy` file in the cloudpath (a failure instead leaves a `*_error.json` sidecar for debugging, so failed neurons are retried):

```python
def filter_tasks(task_df, cloudpath):
    cf = CloudFiles(cloudpath)
    done_ids = np.array(
        [int(f.split(".")[0]) for f in cf.list() if f.endswith(f".{OSSIFY_EXTENSION}")]
    )
    if len(done_ids) == 0:
        return task_df
    return task_df[~task_df["root_id"].isin(done_ids)]
```

This is why `CF_CLOUDPATH` is a deliberate per-run choice: point a new run at the **same** bucket (when the processing code is unchanged) and `filter_tasks` will skip already-assembled neurons, so you resume/extend instead of recomputing. Use a **fresh** bucket only when the computation itself changed and the old outputs must not mask the new ones. If you want different completion semantics, edit `filter_tasks` (e.g. match a different extension); returning `task_df` unchanged disables filtering entirely.

### 2. Workers

Now that you have a queue, you need workers to process the tasks.
Once you have defined the tasks, the same environment files are used to define the workers and no additional configuration is needed.

To run a worker locally (for example, for testing), you can use the `run.py` script with the same environment file sourced.
As before, an easy approach is `poe launch_worker -e <profile>` (e.g. `poe launch_worker -e v1dd`).
Similar to above, this command will expect `config/common.env` and `config/<profile>/task.env` to exist in the root of the project directory, and otherwise you should run via `uv run --frozen run.py`.

#### Dockerizing workers

The Dockerfile is already set up to build and run a worker Docker without extra configuration.
Note that the current file is based on the `latest` tag for `uv` and the selected python version, and you should feel free to update or change those to other specific versions if desired.
To build the Docker image, run `docker buildx build --platform linux/amd64 -t YOUR_TAG .` in the root of the project directory.
If you are going to use this image for a GKE cluster, your tag should be of the form `username/projectname:tag` (with your dockerhub username) and then pushed to dockerhub via `docker push username/projectname:tag`.

To test the Docker image locally, use the `-e/--env` selector just like the other tasks:

```bash
poe launch_worker_docker -e v1dd
```

This runs the worker in the image named by `TASK_DOCKER_IMAGE` (from `config/common.env`) with the profile's environment supplied via `--env-file`, and mounts the credentials the worker needs. It is equivalent to:

```bash
docker run \
    --platform linux/amd64 \
    --env-file config/common.env \
    --env-file config/v1dd/task.env \
    -v "$HOME/.cloudvolume/secrets:/root/.cloudvolume/secrets" \
    -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
    "$TASK_DOCKER_IMAGE"
```

Two credential mounts are needed: `~/.cloudvolume/secrets` supplies the CAVE `*-cave-secret.json` files (and any AWS/queue secrets), and `~/.config/gcloud` supplies your gcloud Application Default Credentials so the container can read/write the `gs://` cloudpath as you do on the host. Without the second mount you get `google.auth.exceptions.DefaultCredentialsError` when the worker first touches GCS. The cluster uses the *same* ADC (see "Cloud credentials" below), so local docker and the cluster authenticate identically — as you.

Note: the `--platform linux/amd64` flag matches the image you build for the (amd64) cluster and lets it run under emulation on an Apple-silicon Mac; on a Linux/amd64 host you can drop it. The task assumes the image has already been built or pulled (see below).

### 3. Deploying workers to Google Cloud

Now you want to spin up a bunch of workers on a Google Kubernetes Engine cluster to churn through tasks.
Note that you will need to install `gcloud` [command line tools](https://cloud.google.com/sdk/docs/install) before starting.
This template helps you define a cluster and deploy workers to it with minimal configuration using a profile's `task.env` and `cluster.env` (plus the shared `config/common.env`).

#### Configuring the cluster and template files

There are two steps: first create the cluster (the compute resources), then deploy the tasks that will run on it.
Both are driven from the profile's config — `poe launch_cluster -e <profile>` creates the cluster and `poe apply_task -e <profile>` deploys the task to it.
Each of these **first re-renders the scripts** from `config/common.env`, `config/<profile>/task.env`, and `config/<profile>/cluster.env` (in that order) into that profile's own `config/<profile>/scripts/` dir (`kube-task.yml`, `make_cluster.sh`, `delete_cluster.sh`) before running — so you never have to remember a separate render step, you can never accidentally launch stale scripts after editing a config, and one profile's scripts can never shadow another's.
The `launch_cluster` command will take several minutes.
If you just want to inspect the generated scripts without launching anything, `poe make_scripts -e <profile>` renders them and stops.

Please double check that the secrets are set up correctly on your computer, such that there is a secret file named for both the CAVE_LOCAL_SERVER and CAVE_GLOBAL_SERVER in your `~/.cloudvolume/secrets` directory.
They will have the same contents as one another.

##### Cloud credentials

`make_cluster.sh` bundles your credentials into a single Kubernetes secret that every worker pod mounts at `/root/.cloudvolume/secrets`:

- the two CAVE `*-cave-secret.json` files (named for `CAVE_GLOBAL_SERVER` / `CAVE_LOCAL_SERVER`),
- `aws-secret.json` (needed to poll an SQS `TASKQUEUE_PATH`),
- your gcloud **Application Default Credentials** (`~/.config/gcloud/application_default_credentials.json`), which `kube-task.yml` points `GOOGLE_APPLICATION_CREDENTIALS` at.

So the workers read/write the `gs://` cloudpath **as you** — the same identity you use locally and in `launch_worker_docker` — rather than as a dedicated service account. This is deliberate: it needs none of the service-account/key-creation IAM you may not hold, only the bucket access you already have, and there is no long-lived key to leak or rotate. Trade-offs to know:

- Your personal credentials sit in the cluster's secret, so anyone with secret-read on that (preemptible, short-lived) cluster could act as you. Fine for a cluster you own and tear down; broader than a scoped service account.
- A collaborator can't reuse your ADC. To hand this pipeline to someone else (or a paper artifact), either they run it under their own ADC, or you switch `make_cluster.sh` back to a shared `google-secret.json` service-account key + Workload Identity.
- If GCS calls fail with a quota-project error, run `gcloud auth application-default set-quota-project ${GOOGLE_PROJECT_NAME}` once so the ADC carries a quota project, then redeploy.

###### How the GCS credential is resolved (and why ADC is a *fallback*)

`cloud-files` picks GCS credentials in this order, and the ADC path is the last resort:

1. `~/.cloudvolume/secrets/<bucket>-google-secret.json` (bucket-specific service-account key), if present
2. `~/.cloudvolume/secrets/google-secret.json` (generic service-account key), if present
3. neither → it defers to `google.auth.default()`, which reads `GOOGLE_APPLICATION_CREDENTIALS` (our ADC file), then the well-known ADC path

So a real `google-secret.json` does **not** fall back to your ADC — it *takes precedence*. We run as your ADC precisely because no `google-secret.json` is present. This means the two coexist: if an admin later gives you a proper service-account key, drop it at `~/.cloudvolume/secrets/google-secret.json` and cloud-files uses it automatically (locally with no change; for the cluster, re-add the `--from-file=.../google-secret.json` line to `make_cluster.sh`). Remove it and you fall back to ADC.

Two rules that follow from how this file is read:

- **It must be a genuine service-account key, not an ADC file.** cloud-files loads it with `from_service_account_file`, which rejects an `authorized_user` (gcloud ADC) file. That is exactly why we route the ADC through `GOOGLE_APPLICATION_CREDENTIALS` + the `application_default_credentials.json` filename instead of renaming it to `google-secret.json`.
- **A valid key, or no file at all — never an empty/corrupt one.** Both `taskqueue` and `cloud-files` do a `json.loads(google-secret.json)['project_id']` *at import* whenever the file merely exists, so a 0-byte or malformed `google-secret.json` crashes the worker before any task runs with `JSONDecodeError: Expecting value: line 1 column 1 (char 0)`. (This is the classic footgun when `gcloud ... keys create` fails on permissions and leaves an empty file behind — just `rm` it.)

**Sanity check before deploying to the cluster:** run `poe launch_worker_docker -e <profile>` first — it uses the identical ADC credential path, so if a task completes there, the cluster will authenticate the same way.

You can sequence these together with `poe deploy_task -e <profile>`, which runs `launch_cluster` then `apply_task` for that profile (each re-rendering the scripts as above).

##### Multiple clusters and kubectl context

If you run more than one profile's cluster at a time, `kubectl` has a single *current context*, and commands hit whichever cluster it points at — so a stray `apply` can land on the wrong cluster, or fail with a control-plane `i/o timeout` if the current context is stale (e.g. a cluster you recreated/deleted). To avoid this, `apply_task` first runs `poe use_context -e <profile>`, which refreshes kubeconfig for that profile's cluster (`gcloud container clusters get-credentials`) and makes it current. You can also run `poe use_context -e <profile>` on its own to switch between clusters without relaunching anything.

#### Selecting deployment parameters

##### Kubernetes deployment parameters

Certain configuration parameters are fully defined by your project (project name, VPC subnetwork), and you have few options to choose.
Others are entirely your choice, like the task name or cluster name, and just affect how you see the task in the UI.
However, a few parameters will need to be tuned to the task at hand, like choosing the type of compute node or resource allocations.

##### Compute type and resource allocation

There are a number of different types of compute node, each with [different CPU and memory resources and costs](https://cloud.google.com/compute/docs/machine-types).
For example, a `e2-standard-4` node has 4 CPU-equivalents and 16GB of RAM.
However, the actual resources available to your task will be slightly less than this, because Kubernetes and system management also take up some resources.
Estimate about 0.5 vCPU and up to 2GiB of RAM for each node will go to this overhead, but it may vary.
You will request both a number of nodes to create and the overall number of tasks to run across all nodes.
Ideally, the tasks will fill up the node resources as much as possible, since you pay per node hour, not per task hour.

Note that resources like memory and CPU can specify both a "request" and a "limit" value.
Generally speaking, the request values the minimum resources needed to put a pod with a task on a node, while the limit value sets a maximum value.
If the pod exceeds a resource limit, it will be terminated and a new one created.

*Tip*: There is no need to set a CPU limit, since you are reserving the whole node and CPU use will eventually come down.
However, it is useful to set a Memory limit, which will make Kubernetes kill a pod (that is, a worker) if it starts hogging too many resources and strangling the other pods.

#### Redeploying workers

You might need to redeploy workers to the cluster after adjusting the resource allocations or number of tasks.
Just run `poe apply_task -e <profile>` again — it re-renders `kube-task.yml` from the (edited) config and reapplies it.

### 4. Tearing down your cluster

Once you are done with your tasks, you should delete your cluster to stop paying for its resources.
Run `poe delete_cluster -e <profile>`; it re-renders the profile's `config/<profile>/scripts/delete_cluster.sh` and deletes the cluster named there, so you always delete the cluster for the profile you name (no dependence on whatever was last rendered).
Alternatively, you can delete the cluster manually in the GCP console.

## Acknowledgements

Many thanks to Forrest Collman for piloting task distribution through task-queue, Ben Pedigo for allowing me to shamelessly copy his work in forming the basis of this template, and Will Silversmith for building amazing tools like python-task-queue and cloud-files.
