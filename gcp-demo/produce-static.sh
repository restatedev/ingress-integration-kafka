#!/usr/bin/env bash
# STATIC path (repeatable): reset the Counter, produce values 1..5 to the static
# topic (key=user-1), and verify Counter/user-1/get == 15.
#
# The reset makes this safe to run repeatedly (Counter accumulates otherwise).
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
require_cmd kubectl envsubst

step "Reset Counter/user-1, then produce 1..5 to ${TOPIC_STATIC}"
# Counter/reset and Counter/get take NO input, so Restate requires no content-type
# / empty body (sending application/json => 400 "expected body ... to be empty").
retry 5 3 kubectl -n "${NAMESPACE}" run counter-reset \
  --rm -i --restart=Never --image="${CURL_IMAGE}" --env="K=user-1" --command -- \
  sh -c 'curl -fsS -X POST "http://restate:8080/Counter/$K/reset"' \
  || die "could not reset Counter"

run_producer_job 50-producer-static.yaml kafka-producer-static

step "Verify Counter/user-1/get == 15"
if verify_pod verify-static \
     'for i in $(seq 1 60); do v=$(curl -sS -X POST "http://restate:8080/Counter/$K/get"); echo "attempt $i => [$v]" >&2; [ "$v" = "15" ] && { echo "$v"; exit 0; }; sleep 3; done; exit 1' \
     --env="K=user-1"
then rc=0; else rc=1; fi

log_tail ingress-static 30

if [ "${rc}" -eq 0 ]; then ok "Counter is 15 — static path works 🎉"
else die "Counter never reached 15 (see ingress-static logs above)"
fi
