#!/usr/bin/env bash
# DYNAMIC path (repeatable): produce JSON records to the dynamic topic. The dynamic
# ingress parses each value and extracts the virtual-object key from the /key JSON
# pointer, routing to MapObject/set. Then verify the entries landed.
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/lib.sh"
require_cmd kubectl envsubst

step "Produce JSON to ${TOPIC_DYNAMIC} (VO key extracted from /key)"
run_producer_job 55-producer-dynamic.yaml kafka-producer-dynamic

step "Verify MapObject/alice=hello and MapObject/bob=world"
if verify_pod verify-dynamic \
     'for i in $(seq 1 60); do
        a=$(curl -sS -X POST "http://restate:8080/MapObject/$K1/get" -H "content-type: application/json" -d "\"$K1\"");
        b=$(curl -sS -X POST "http://restate:8080/MapObject/$K2/get" -H "content-type: application/json" -d "\"$K2\"");
        echo "attempt $i => $K1=[$a] $K2=[$b]" >&2;
        case "$a" in *"$V1"*) case "$b" in *"$V2"*) echo ok; exit 0;; esac;; esac;
        sleep 3;
      done; exit 1' \
     --env="K1=alice" --env="V1=hello" \
     --env="K2=bob" --env="V2=world"
then rc=0; else rc=1; fi

log_tail ingress-dynamic 30

if [ "${rc}" -eq 0 ]; then ok "MapObject has both entries — dynamic key extraction works 🎉"
else die "MapObject entries not found (see ingress-dynamic logs above)"
fi
