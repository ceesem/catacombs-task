# comment
gcloud config set project ${GOOGLE_PROJECT_NAME}

gcloud container --project "${GOOGLE_PROJECT_NAME}" clusters create "$GOOGLE_CLUSTER_NAME" \
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

gcloud container clusters get-credentials --zone ${GOOGLE_ZONE} ${GOOGLE_CLUSTER_NAME}

# https://kubernetes.io/docs/concepts/configuration/secret/
# Cloud-storage credentials: instead of a service-account key (google-secret.json),
# we bake in your gcloud Application Default Credentials so the workers run as YOU
# — same identity you use locally. kube-task.yml points GOOGLE_APPLICATION_CREDENTIALS
# at the mounted application_default_credentials.json. See HOW_TO_USE_TASKS.md.
kubectl create secret generic secrets \
    --from-file=$HOME/.cloudvolume/secrets/${CAVE_GLOBAL_SERVER}-cave-secret.json \
    --from-file=$HOME/.cloudvolume/secrets/${CAVE_LOCAL_SERVER}-cave-secret.json \
    --from-file=$HOME/.cloudvolume/secrets/aws-secret.json \
    --from-file=$HOME/.config/gcloud/application_default_credentials.json