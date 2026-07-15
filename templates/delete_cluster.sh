#!/bin/bash
#
# Rendered per profile into config/<profile>/scripts/delete_cluster.sh. Deletes
# the profile's GKE cluster and ALL its resources. Zone/project are pinned so it
# targets exactly this profile's cluster; gcloud still prompts for confirmation.
set -euo pipefail

gcloud container clusters delete "${GOOGLE_CLUSTER_NAME}" \
    --zone "${GOOGLE_ZONE}" --project "${GOOGLE_PROJECT_NAME}"
