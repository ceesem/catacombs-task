#!/bin/bash
#
# Rendered per profile into config/<profile>/scripts/make_cluster.sh by
# scripts/process_template.sh (poe make_scripts / launch_cluster). Creates the
# profile's GKE cluster and installs the `secrets` secret its pods mount.
#
# set -e is load-bearing: if get-credentials below fails (e.g. the cluster is
# still PROVISIONING), the script MUST abort rather than fall through to the
# kubectl steps — otherwise they run against whatever kube context happens to be
# current (another profile's cluster), which silently misdirects the secret.
#
# Safe to re-run: cluster create is skipped if the cluster already exists (its
# node pool is instead resized to NUMBER_NODES), and the secret is upserted, so
# re-running repairs a missing/wrong secret and reconciles the node count.
#
# NOTE: this template is rendered by scripts/process_template.sh via bare
# `envsubst`, which bakes every ${VAR} that is set in the env and BLANKS any it
# does not find. So everything below must be either a literal or an env var that
# the profile actually sets — no script-local shell variables (they would render
# to empty strings). The node pool is hardcoded "default-pool" because the
# create call below names no pool, so GKE always calls it that.
set -euo pipefail

gcloud config set project ${GOOGLE_PROJECT_NAME}

if gcloud container clusters describe "${GOOGLE_CLUSTER_NAME}" \
    --zone "${GOOGLE_ZONE}" --project "${GOOGLE_PROJECT_NAME}" >/dev/null 2>&1; then
  # Cluster exists: reconcile the node count to NUMBER_NODES instead of creating.
  # resize is a no-op when the pool is already at that size, so this stays
  # idempotent; --num-nodes is the desired TOTAL, not a delta. --quiet skips the
  # interactive confirm so `poe launch_cluster` runs unattended.
  echo "Cluster ${GOOGLE_CLUSTER_NAME} already exists; resizing default-pool to ${NUMBER_NODES} nodes."
  gcloud container clusters resize "${GOOGLE_CLUSTER_NAME}" \
      --node-pool default-pool \
      --num-nodes "${NUMBER_NODES}" \
      --zone "${GOOGLE_ZONE}" --project "${GOOGLE_PROJECT_NAME}" \
      --quiet
else
  gcloud container --project "${GOOGLE_PROJECT_NAME}" clusters create "${GOOGLE_CLUSTER_NAME}" \
      --zone "${GOOGLE_ZONE}" \
      --no-enable-basic-auth \
      --release-channel "stable" \
      --machine-type "${GOOGLE_COMPUTE_TYPE}" \
      --image-type "COS_CONTAINERD" \
      --disk-type "pd-standard" \
      --disk-size "600" \
      --metadata disable-legacy-endpoints=true \
      --scopes "https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" \
      --preemptible \
      --num-nodes "${NUMBER_NODES}" \
      --logging=SYSTEM,WORKLOAD \
      --monitoring=SYSTEM \
      --enable-ip-alias \
      --network "projects/${GOOGLE_PROJECT_NAME}/global/networks/${GOOGLE_VPC_NETWORK_NAME}" \
      --subnetwork "projects/${GOOGLE_PROJECT_NAME}/regions/${GOOGLE_SUBNETWORK_REGION}/subnetworks/${GOOGLE_VPC_NETWORK_NAME}" \
      --no-enable-intra-node-visibility \
      --no-enable-master-authorized-networks \
      --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver \
      --enable-autoupgrade \
      --enable-autorepair \
      --max-unavailable-upgrade 0 \
      --max-pods-per-node "${PODS_PER_NODE}" \
      --enable-shielded-nodes \
      --shielded-secure-boot \
      --shielded-integrity-monitoring \
      --node-locations "${GOOGLE_ZONE}"
fi

# Refresh kubeconfig for THIS cluster. Under set -e a failure here aborts the
# script before the kubectl steps below.
gcloud container clusters get-credentials "${GOOGLE_CLUSTER_NAME}" \
    --zone "${GOOGLE_ZONE}" --project "${GOOGLE_PROJECT_NAME}"

# https://kubernetes.io/docs/concepts/configuration/secret/
# Cloud-storage credentials: instead of a service-account key (google-secret.json),
# we bake in your gcloud Application Default Credentials so the workers run as YOU
# — same identity you use locally. kube-task.yml points GOOGLE_APPLICATION_CREDENTIALS
# at the mounted application_default_credentials.json. See HOW_TO_USE_TASKS.md.
#
# --context is pinned to this profile's cluster (not "current context") so the
# secret can never land on the wrong cluster. `create --dry-run | apply` upserts
# it, so re-running is safe and doesn't leave a window with no secret.
kubectl --context "gke_${GOOGLE_PROJECT_NAME}_${GOOGLE_ZONE}_${GOOGLE_CLUSTER_NAME}" \
    create secret generic secrets \
    --from-file=$HOME/.cloudvolume/secrets/${CAVE_GLOBAL_SERVER}-cave-secret.json \
    --from-file=$HOME/.cloudvolume/secrets/${CAVE_LOCAL_SERVER}-cave-secret.json \
    --from-file=$HOME/.cloudvolume/secrets/aws-secret.json \
    --from-file=$HOME/.config/gcloud/application_default_credentials.json \
    --dry-run=client -o yaml \
  | kubectl --context "gke_${GOOGLE_PROJECT_NAME}_${GOOGLE_ZONE}_${GOOGLE_CLUSTER_NAME}" apply -f -
