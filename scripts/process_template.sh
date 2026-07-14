#!/bin/bash
#
# Render templates/* -> scripts/* by sourcing one or more env files (in order,
# later files override earlier) and running envsubst over each template.
#
# Usage: process_template.sh <env_file> [<env_file> ...]
#   Typical: config/common.env config/<env>/task.env config/<env>/cluster.env
#   (driven by `poe make_scripts --env <env>`)

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <env_file> [<env_file> ...]"
  exit 1
fi

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
  if ! envsubst < "templates/$tmpl" > "scripts/$tmpl"; then
    echo "Error: failed to run envsubst on templates/$tmpl"
    exit 1
  fi
done

echo "Templates configured successfully from: $*"
exit 0
