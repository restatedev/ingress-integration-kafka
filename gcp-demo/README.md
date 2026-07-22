# gcp-demo — Kafka → Restate on GKE, against Google Cloud Managed Kafka

A self-contained demo that runs the whole stack **on GKE, inside the same VPC as a
real [Google Cloud Managed Service for Apache Kafka](https://cloud.google.com/managed-service-for-apache-kafka)
cluster**:

- **Restate** (your image — must include the ingestion gRPC API),
- **test-services** (the Java SDK `Counter` + `MapObject` virtual objects, from ghcr.io),
- **two copies of this repo's ingress**, layered with the GCP Kafka **SASL/OAUTHBEARER**
  auth handler, authenticating to Managed Kafka via **Workload Identity** (no key files).

Two topics show the two record mappers:

| Topic            | Ingress          | Mapper                          | What it demonstrates                                              |
|------------------|------------------|---------------------------------|------------------------------------------------------------------|
| `counter-topic`  | `ingress-static` | `StaticRecordMapper` (default)  | Kafka key → VO key, value → payload → `Counter/add`. Sum → 15.    |
| `map-topic`      | `ingress-dynamic`| `JsonDynamicTargetRecordMapper` | **VO key extracted from the JSON payload** (`/key`) → `MapObject/set`. |

### Why GKE and not `docker compose` on your laptop?

Managed Kafka has **no public endpoint** — the produce/consume data plane is
private-VPC-only, and the bootstrap DNS resolves to a private IP inside the VPC.
GKE nodes live in that VPC, so connectivity and DNS "just work", and pods use
Workload Identity for auth instead of mounted credentials. Producing has the same
constraint (the Cloud console has no "publish message" button), which is why records
are sent by in-cluster producer Jobs.

---

## One-time setup

1. **Edit `config.env`** — set at least `PROJECT_ID`, `REGION`, `RESTATE_IMAGE`,
   `INGRESS_IMAGE`. Everything else has defaults.

2. **Log in** and make sure `gcloud`, `kubectl`, and `envsubst` (gettext) are installed:
   ```bash
   gcloud auth login
   gcloud components install gke-gcloud-auth-plugin   # if kubectl auth complains
   ```

3. **Build & push the two images you own** into your Artifact Registry.
   - Ingress: builds the base (`./gradlew jibDockerBuild`), layers the GCP auth
     handler, and pushes — all in one:
     ```bash
     ./gcp-demo/build-images.sh ingress   # gradle base -> layer auth -> push $INGRESS_IMAGE
     ```
     (Set `SKIP_BASE_BUILD=1` to reuse an already-built base image.)
   - Restate: built from source at `../restate` (`docker build`, must include the
     ingestion gRPC API `restate.ingestion.v1`) and pushed:
     ```bash
     ./gcp-demo/build-images.sh restate   # docker build ../restate -> push $RESTATE_IMAGE
     ```
   - Or both at once: `./gcp-demo/build-images.sh`.

4. **Provision + deploy (run once):**
   ```bash
   cd gcp-demo
   `./setup.sh`
   ```
   This provisions GKE + Managed Kafka + IAM, deploys the stack, and registers the
   services. The Managed Kafka cluster is the slow part (~20-30 min). It's idempotent,
   so re-runs skip what already exists.

## Produce + verify (repeatable)

Two independent scripts, one per path — run either as often as you like:

```bash
./produce-static.sh     # counter-topic -> Counter; resets, produces 1..5, asserts get == 15
./produce-dynamic.sh    # map-topic     -> MapObject; produces JSON, asserts the entries
```

`produce-static.sh` resets the Counter before producing, so the `== 15` assertion
holds on every run. Expected finish:

```
 ✓ Counter is 15 — static path works 🎉
 ✓ MapObject has both entries — dynamic key extraction works 🎉
```

## Inspect

```bash
./ui.sh                                                   # port-forward the Restate Web UI -> http://localhost:9070/ui/
kubectl -n restate-demo get pods
kubectl -n restate-demo logs -f deploy/ingress-static     # static path + Kafka client logs
kubectl -n restate-demo logs -f deploy/ingress-dynamic    # dynamic path
kubectl -n restate-demo logs job/kafka-producer-static
kubectl -n restate-demo logs job/kafka-producer-dynamic
```

## Tear down (run once)

```bash
./teardown.sh          # k8s + GKE + Kafka + IAM binding
./teardown.sh k8s      # just the k8s namespace, leave GCP infra running
```

---

## How it fits together

```
Managed Kafka (private, VPC)  ──SASL/OAUTHBEARER (ADC via Workload Identity)──┐
  ├─ counter-topic                                                            │
  └─ map-topic                                                                │
                                                                             │
GKE (Autopilot, same VPC)                                                    │
  ├─ ingress-static   (StaticRecordMapper)  ─── counter-topic ───────────────┤
  ├─ ingress-dynamic  (JsonDynamicTargetRecordMapper) ─ map-topic ───────────┘
  │        │  both run as KSA ↔ GSA[roles/managedkafka.client]
  │        ▼
  ├─ restate (ingress :8080 + admin :9070, PVC)
  │        │  invokes
  │        ▼
  └─ test-services (Counter + MapObject VOs, :9080)

  producer Jobs (one-off):
    kafka-producer-static  -> counter-topic: key=user-1, values 1..5
    kafka-producer-dynamic -> map-topic:     {"key":"alice","value":"hello"}, {"key":"bob",...}
```

- **Static path:** `counter-topic` record `key=user-1 value=3` → `Counter("user-1").add(3)`.
- **Dynamic path:** `map-topic` record `{"key":"alice","value":"hello"}` → the mapper reads
  `/key` for the VO key (`alice`) and sends the whole JSON as the payload to
  `MapObject("alice").set(...)`. (For `MapObject`, the VO key and the map entry key are
  the same field — that's just how that test service is shaped.)
- **Auth:** the Kubernetes SA `kafka-ingress` is annotated
  `iam.gke.io/gcp-service-account: <GSA>`; the GSA holds `roles/managedkafka.client`
  and is bound to the KSA via `roles/iam.workloadIdentityUser`. The
  `GcpLoginCallbackHandler` fetches a token through ADC → the GKE metadata server.

## Notes / gotchas

- **`gcloud managed-kafka`** must be available (Managed Kafka is GA). On an older SDK,
  update it (`gcloud components update`) or use the `beta` surface.
- **Same VPC/subnet** for GKE and Kafka is required so the private bootstrap DNS
  resolves from pods. Both default to `default`/`default` in `config.env`.
- **Bootstrap address** is derived as
  `bootstrap.<cluster>-<region>.managedkafka.<project>.cloud.goog:9092`. If your cluster
  reports a different one (console → cluster → Configuration), set `KAFKA_BOOTSTRAP`.
- **Restate image** must carry the ingestion gRPC service, or the ingress streams fail
  with `UNIMPLEMENTED`.
```
files:
  config.env              # edit me
  setup.sh                # once: provision + deploy + register
  produce-static.sh       # repeatable: counter-topic -> Counter
  produce-dynamic.sh      # repeatable: map-topic -> MapObject (dynamic key)
  teardown.sh             # once: delete everything
  ui.sh                   # port-forward the Restate Web UI / admin / ingress to localhost
  build-images.sh         # build+push the ingress & restate images  [all|ingress|restate]
  Dockerfile.gcp          # base ingress + GCP auth handler
  lib.sh                  # shared config/render/helpers
  k8s/                    # namespace, KSA, restate, test-services, 2x ingress, 2x producer
```
