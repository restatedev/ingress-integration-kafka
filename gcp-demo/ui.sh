#!/usr/bin/env bash
# Port-forward the Restate Web UI / admin API (and the ingress) to localhost.
#
#   ./ui.sh                 # forwards 9070 (UI/admin) + 8080 (ingress)
#   ADMIN_PORT=9090 ./ui.sh # use a different local port for the UI/admin
#
# Then open the UI:  http://localhost:9070/ui/
# Leave it running; Ctrl-C to stop.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
require_cmd kubectl

ADMIN_PORT="${ADMIN_PORT:-9070}"     # local port -> restate admin/UI (9070 in-cluster)
INGRESS_PORT="${INGRESS_PORT:-8080}" # local port -> restate ingress (8080 in-cluster)

# Wait for the restate pod so the forward doesn't immediately drop on a fresh cluster.
step "Waiting for Restate to be ready"
kubectl -n "${NAMESPACE}" rollout status deploy/restate --timeout=120s

step "Port-forwarding Restate"
info "Web UI:          http://localhost:${ADMIN_PORT}/ui/"
info "Admin API:       http://localhost:${ADMIN_PORT}   (e.g. /deployments, /openapi)"
info "Ingress (HTTP):  http://localhost:${INGRESS_PORT}  (e.g. POST /Counter/user-1/get)"
info "Ctrl-C to stop."
exec kubectl -n "${NAMESPACE}" port-forward svc/restate \
  "${ADMIN_PORT}:9070" "${INGRESS_PORT}:8080"
