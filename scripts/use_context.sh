#!/bin/bash
#
# Point kubectl at the GKE cluster named by a profile's env files, by refreshing
# kubeconfig credentials (which also sets it as the current context). This keeps
# kubectl aimed at the profile you name — important when juggling more than one
# cluster — and repairs a stale control-plane endpoint after a recreate/resize.
#
# Usage: use_context.sh <env_file> [<env_file> ...]
#   Typical: config/common.env config/<env>/cluster.env
#   (GOOGLE_ZONE / GOOGLE_PROJECT_NAME come from common.env; GOOGLE_CLUSTER_NAME
#    from the profile's cluster.env)

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <env_file> [<env_file> ...]"
  exit 1
fi

set -a
for env_file in "$@"; do
  if [[ ! -r "$env_file" ]]; then
    echo "Error: environment file '$env_file' not found or not readable."
    exit 1
  fi
  source "$env_file"
done
set +a

gcloud container clusters get-credentials "$GOOGLE_CLUSTER_NAME" \
  --zone "$GOOGLE_ZONE" \
  --project "$GOOGLE_PROJECT_NAME"
