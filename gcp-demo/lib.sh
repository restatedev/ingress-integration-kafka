#!/usr/bin/env bash
# Shared helpers for the gcp-demo scripts. Source this; don't execute it.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# --- config ---------------------------------------------------------------
# shellcheck source=/dev/null
source "${HERE}/config.env"

# Bootstrap address: prefer the pinned config value, else ask the API for the
# authoritative one, else fall back to the documented format. (The API is the
# source of truth: the format is bootstrap.<cluster>.<region>.managedkafka.<project>.cloud.goog:9092.)
if [ -z "${KAFKA_BOOTSTRAP:-}" ]; then
  if command -v gcloud >/dev/null 2>&1; then
    KAFKA_BOOTSTRAP="$(gcloud managed-kafka clusters describe "${KAFKA_CLUSTER}" \
        --location="${REGION}" --project="${PROJECT_ID}" \
        --format='value(bootstrapAddress)' 2>/dev/null || true)"
  fi
  : "${KAFKA_BOOTSTRAP:=bootstrap.${KAFKA_CLUSTER}.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092}"
fi

SUBNET_FQN="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET}"

# --- pretty logging -------------------------------------------------------
_c() { printf '\033[%sm' "$1"; }
step() { printf '\n%s==>%s %s\n' "$(_c '1;34')" "$(_c 0)" "$*"; }
info() { printf '    %s\n' "$*"; }
ok()   { printf '%s ✓ %s%s\n' "$(_c '1;32')" "$*" "$(_c 0)"; }
die()  { printf '%s ✗ %s%s\n' "$(_c '1;31')" "$*" "$(_c 0)" >&2; exit 1; }

require_cmd() { for c in "$@"; do command -v "$c" >/dev/null 2>&1 || die "missing required command: $c"; done; }
require_var() {
  for v in "$@"; do
    [ -n "${!v:-}" ] || die "config.env: '$v' is not set (edit gcp-demo/config.env)"
  done
}

# --- kubectl / gcloud convenience ----------------------------------------
K() { kubectl -n "${NAMESPACE}" "$@"; }
G() { gcloud --project="${PROJECT_ID}" "$@"; }

# --- manifest rendering ---------------------------------------------------
# Only these names are substituted; every other `$foo` in the YAML (e.g. shell
# vars inside container scripts like $CLASSPATH) is left untouched.
RENDER_VARS='${NAMESPACE} ${KSA_NAME} ${GSA_EMAIL} ${RESTATE_IMAGE} ${RESTATE_STORAGE_SIZE} ${TEST_SERVICES_IMAGE} ${TEST_SERVICES} ${INGRESS_IMAGE} ${KAFKA_BOOTSTRAP} ${TOPIC_STATIC} ${TOPIC_DYNAMIC} ${MAVEN_IMAGE} ${KAFKA_CLI_IMAGE} ${AUTH_HANDLER_VERSION}'

render() {
  export NAMESPACE KSA_NAME GSA_EMAIL RESTATE_IMAGE RESTATE_STORAGE_SIZE \
    TEST_SERVICES_IMAGE TEST_SERVICES INGRESS_IMAGE KAFKA_BOOTSTRAP \
    TOPIC_STATIC TOPIC_DYNAMIC MAVEN_IMAGE KAFKA_CLI_IMAGE AUTH_HANDLER_VERSION
  envsubst "${RENDER_VARS}" < "$1"
}

apply() { render "${HERE}/k8s/$1" | kubectl apply -f -; }

# Retry <tries> <sleep-seconds> <cmd...>; returns non-zero if all attempts fail.
retry() {
  local tries="$1" nap="$2"; shift 2
  local n=0
  until "$@"; do
    n=$((n + 1))
    if [ "$n" -ge "$tries" ]; then return 1; fi
    sleep "$nap"
  done
}

# Run a producer Job manifest and block until it succeeds (or fails/times out).
#   run_producer_job <manifest-file> <job-name>
run_producer_job() {
  local manifest="$1" job="$2" s f
  K delete job "${job}" --ignore-not-found >/dev/null
  apply "${manifest}"
  for _ in $(seq 1 100); do
    s="$(K get job "${job}" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
    f="$(K get job "${job}" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
    if [ "${s:-0}" = "1" ]; then break; fi
    if [ "${f:-0}" != "" ] && [ "${f:-0}" -ge 2 ] 2>/dev/null; then
      K logs "job/${job}" || true
      die "${job} failed"
    fi
    sleep 3
  done
  K logs "job/${job}" || true
  [ "${s:-0}" = "1" ] || die "${job} did not complete in time"
}

# Run a short-lived curl pod: verify_pod <name> <sh-script> [extra kubectl run args...]
# (extra args, e.g. --env="K=v", are passed to `kubectl run`). Returns the pod's
# exit code, so the sh script can `exit 0`/`exit 1` to signal pass/fail.
verify_pod() {
  local name="$1" script="$2"; shift 2
  kubectl -n "${NAMESPACE}" run "${name}" --rm -i --restart=Never \
    --image="${CURL_IMAGE}" "$@" --command -- sh -c "${script}"
}

# Print the tail of a deployment's logs (best-effort; never fails the caller).
log_tail() { # log_tail <deployment> [lines]
  local dep="$1" n="${2:-25}"
  step "Recent logs: deploy/${dep} (last ${n} lines)"
  K logs "deploy/${dep}" --tail="${n}" 2>&1 || true
}

# Ensure the Artifact Registry Docker repo exists (idempotent).
ensure_ar_repo() {
  G services enable artifactregistry.googleapis.com >/dev/null
  if ! G artifacts repositories describe "${AR_REPO}" --location="${REGION}" >/dev/null 2>&1; then
    step "Creating Artifact Registry repo ${AR_REPO}"
    G artifacts repositories create "${AR_REPO}" \
      --repository-format=docker --location="${REGION}" \
      --description="restate gcp-demo images"
  fi
}
