#!/usr/bin/env bash
# Delete everything setup.sh created. Safe to run repeatedly.
#
#   ./teardown.sh          # delete k8s workloads + GKE + Kafka + IAM binding
#   ./teardown.sh k8s      # only the k8s namespace (leave GCP infra up)
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
require_cmd gcloud
require_var PROJECT_ID REGION

teardown_k8s() {
  step "Deleting namespace ${NAMESPACE}"
  kubectl delete namespace "${NAMESPACE}" --ignore-not-found --wait=false || true
}

teardown_gcp() {
  step "Deleting GKE cluster ${GKE_CLUSTER}"
  G container clusters delete "${GKE_CLUSTER}" --region="${REGION}" --quiet || true

  step "Deleting Managed Kafka cluster ${KAFKA_CLUSTER}"
  G managed-kafka clusters delete "${KAFKA_CLUSTER}" --location="${REGION}" --quiet || true

  step "Removing IAM binding + service account"
  G projects remove-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${GSA_EMAIL}" \
    --role="roles/managedkafka.client" --condition=None --quiet >/dev/null 2>&1 || true
  G iam service-accounts delete "${GSA_EMAIL}" --quiet || true

  info "Left in place (delete by hand if you want): Artifact Registry repo '${AR_REPO}' and its images."
}

case "${1:-all}" in
  k8s) teardown_k8s ;;
  all) teardown_k8s; teardown_gcp ;;
  *) die "usage: $0 [all|k8s]" ;;
esac
ok "teardown complete"
