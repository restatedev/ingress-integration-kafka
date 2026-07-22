#!/usr/bin/env bash
# One-time setup: provision GCP (GKE + Managed Kafka + IAM), deploy the stack,
# and register the services with Restate. Run this ONCE. Then use
# ./produce-static.sh / ./produce-dynamic.sh (repeatable) and ./teardown.sh to clean up.
#
# It is idempotent, so it's safe to re-run if something fails partway.
#
# Prereqs (see README.md): edit config.env, `gcloud auth login`, and push your
# Restate + ingress images (./build-images.sh handles the ingress one).
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

preflight() {
  step "Preflight"
  require_cmd gcloud kubectl envsubst
  require_var PROJECT_ID REGION RESTATE_IMAGE INGRESS_IMAGE
  gcloud config set project "${PROJECT_ID}" >/dev/null
  info "project=${PROJECT_ID} region=${REGION}"
  info "bootstrap=${KAFKA_BOOTSTRAP}"
  info "topics=${TOPIC_STATIC} (static), ${TOPIC_DYNAMIC} (dynamic)"
  local img ref
  for img in RESTATE_IMAGE INGRESS_IMAGE; do
    ref="${!img}"
    if ! gcloud artifacts docker images describe "${ref}" >/dev/null 2>&1; then
      info "WARN: can't confirm ${img} exists yet: ${ref}"
      info "      push it first (RESTATE by you; INGRESS via ./build-images.sh)"
    fi
  done
  ok "preflight done"
}

provision() {
  step "Enabling APIs"
  G services enable \
    container.googleapis.com managedkafka.googleapis.com \
    artifactregistry.googleapis.com compute.googleapis.com iam.googleapis.com
  ensure_ar_repo

  # Kick off the slow Kafka cluster create first, then build GKE while it cooks.
  if G managed-kafka clusters describe "${KAFKA_CLUSTER}" --location="${REGION}" >/dev/null 2>&1; then
    info "Kafka cluster ${KAFKA_CLUSTER} already exists"
  else
    step "Creating Managed Kafka cluster ${KAFKA_CLUSTER} (async)"
    G managed-kafka clusters create "${KAFKA_CLUSTER}" \
      --location="${REGION}" \
      --cpu="${KAFKA_CPU}" --memory="${KAFKA_MEMORY}" \
      --subnets="${SUBNET_FQN}" \
      --async
  fi

  if G container clusters describe "${GKE_CLUSTER}" --region="${REGION}" >/dev/null 2>&1; then
    info "GKE cluster ${GKE_CLUSTER} already exists"
  else
    step "Creating GKE Autopilot cluster ${GKE_CLUSTER} (Workload Identity on by default)"
    G container clusters create-auto "${GKE_CLUSTER}" \
      --region="${REGION}" --network="${NETWORK}" --subnetwork="${SUBNET}"
  fi

  step "Fetching kubeconfig for ${GKE_CLUSTER}"
  G container clusters get-credentials "${GKE_CLUSTER}" --region="${REGION}"

  step "Service account + IAM (Workload Identity)"
  if ! G iam service-accounts describe "${GSA_EMAIL}" >/dev/null 2>&1; then
    G iam service-accounts create "${GSA_NAME}" --display-name="restate gcp-demo ingress"
  fi
  info "granting roles/managedkafka.client to ${GSA_EMAIL}"
  G projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${GSA_EMAIL}" \
    --role="roles/managedkafka.client" --condition=None >/dev/null
  info "binding KSA ${NAMESPACE}/${KSA_NAME} -> ${GSA_EMAIL}"
  G iam service-accounts add-iam-policy-binding "${GSA_EMAIL}" \
    --role="roles/iam.workloadIdentityUser" \
    --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]" >/dev/null

  step "Waiting for Managed Kafka cluster to become ACTIVE (typically 20-30 min)"
  local st
  for _ in $(seq 1 120); do
    st="$(G managed-kafka clusters describe "${KAFKA_CLUSTER}" --location="${REGION}" \
          --format='value(state)' 2>/dev/null || true)"
    info "state: ${st:-<none>}"
    if [ "${st}" = "ACTIVE" ]; then break; fi
    sleep 30
  done
  [ "${st}" = "ACTIVE" ] || die "Kafka cluster not ACTIVE in time"
  ok "Kafka cluster ACTIVE"

  local t
  for t in "${TOPIC_STATIC}" "${TOPIC_DYNAMIC}"; do
    if G managed-kafka topics describe "${t}" \
         --cluster="${KAFKA_CLUSTER}" --location="${REGION}" >/dev/null 2>&1; then
      info "topic ${t} already exists"
    else
      step "Creating topic ${t}"
      G managed-kafka topics create "${t}" \
        --cluster="${KAFKA_CLUSTER}" --location="${REGION}" \
        --partitions="${TOPIC_PARTITIONS}" --replication-factor="${TOPIC_REPLICATION}"
    fi
  done
  ok "provision done"
}

deploy() {
  step "Applying manifests"
  apply 00-namespace.yaml
  apply 10-serviceaccount.yaml
  apply 20-restate.yaml
  apply 30-test-services.yaml
  apply 40-ingress-static.yaml
  apply 45-ingress-dynamic.yaml

  step "Waiting for rollouts"
  K rollout status deploy/restate --timeout=300s
  K rollout status deploy/test-services --timeout=300s
  K rollout status deploy/ingress-static --timeout=300s
  K rollout status deploy/ingress-dynamic --timeout=300s

  step "Registering test-services (Counter + MapObject) with Restate"
  if retry 10 6 kubectl -n "${NAMESPACE}" run registrar \
       --rm -i --restart=Never --image="${CURL_IMAGE}" --command -- \
       sh -c 'curl -fsS -X POST http://restate:9070/deployments -H "content-type: application/json" -d "{\"uri\":\"http://test-services:9080\",\"force\":true}"'
  then ok "deployment registered"
  else die "could not register test-services with Restate admin"
  fi
  ok "deploy done"
}

preflight
provision
deploy

step "Setup complete."
info "Static path (repeatable):   ./produce-static.sh"
info "Dynamic path (repeatable):  ./produce-dynamic.sh"
info "Ingress logs:               kubectl -n ${NAMESPACE} logs -f deploy/ingress-static"
info "                        kubectl -n ${NAMESPACE} logs -f deploy/ingress-dynamic"
info "Tear it all down:       ./teardown.sh"
