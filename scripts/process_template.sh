#!/bin/bash
#
# Render templates/* -> config/<profile>/scripts/* by sourcing one or more env
# files (in order, later files override earlier) and running envsubst over each
# template. Each profile renders into its OWN scripts dir so one profile's
# outputs can never shadow another's (a shared scripts/ dir made it ambiguous
# which profile the on-disk scripts belonged to).
#
# Usage: process_template.sh <profile> <env_file> [<env_file> ...]
#   Typical: v1dd_v1512 config/common.env config/v1dd_v1512/task.env config/v1dd_v1512/cluster.env
#   (driven by `poe make_scripts -e <profile>`)

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <profile> <env_file> [<env_file> ...]"
  exit 1
fi

profile="$1"
shift
outdir="config/$profile/scripts"
mkdir -p "$outdir"

# Source every env file in order so later files override earlier ones.
set -a
for env_file in "$@"; do
  if [[ ! -r "$env_file" ]]; then
    echo "Error: environment file '$env_file' not found or not readable."
    exit 1
  fi
  if ! source "$env_file"; then
    echo "Error: failed to source environment file '$env_file'."
    exit 1
  fi
done
set +a

for tmpl in kube-task.yml make_cluster.sh delete_cluster.sh; do
  if [[ ! -r "templates/$tmpl" ]]; then
    echo "Error: template file templates/$tmpl does not exist or is not readable"
    exit 1
  fi
  if ! envsubst < "templates/$tmpl" > "$outdir/$tmpl"; then
    echo "Error: failed to run envsubst on templates/$tmpl"
    exit 1
  fi
done

echo "Templates rendered to $outdir from: $*"
exit 0
