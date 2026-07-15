# Restate Kafka ingress integration

An external, opinionated Kafka consumer that streams records into [Restate](https://restate.dev)
through the new **Ingress Integration API** (`restate.ingestion.IngestionSvc`, gRPC). It replaces
the built-in librdkafka consumer: it runs as its own process/container, uses the official Apache
Kafka Java client, and speaks a **pull-based** protocol so Restate controls the flow.

Built with Kotlin coroutines on **Vert.x 5** (Java 25).

## How it works

- One gRPC `Ingest` stream per **(topic, partition)**. Each stream is one Restate *producer*.
- **Pull-based**: Restate sends flow-control *window* credits; the consumer only fetches from a
  Kafka partition while it has credit, pausing it otherwise. Records beyond the current window are
  briefly buffered (a Kafka poll returns batches), so backpressure is precise to ~one poll batch.
- **Exactly-once into Restate**: an offset is committed back to Kafka only after Restate confirms
  it (`last_committed`). Restate deduplicates by `(producer_id, offset)`, where
  `producer_id = "<group.id>/<topic>/<partition>"`, so re-sends after a restart/reconnect are safe.
- **Ordering**: per-partition order is preserved (records are sent in offset order).
- **Concurrency**: one process runs *N* consumer instances (one per event loop). They share one
  Kafka `group.id`, so Kafka distributes the partitions across them — scale by increasing instances
  and/or partitions, or by running more containers.

## Record mapping (parity with the built-in consumer)

| Kafka | Restate `Record` |
|-------|------------------|
| key (UTF-8) | `key` (the Virtual Object / Workflow key) |
| value (bytes) | `payload` (tombstone/null → empty payload) |
| offset | `offset` |
| topic / partition / offset / timestamp / key | `additional_headers`: `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp`, `kafka.key` |
| `traceparent` / `tracestate` headers | propagated to `traceparent` / `tracestate` |

All topics route to a single target `service`/`handler`. No `content-type` header is set.

## Quickstart

```bash
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e KAFKA_GROUP_ID=my-consumer \
  -e KAFKA_TOPICS=orders,payments \
  -e RESTATE_INGRESS_URL=http://restate:8080 \
  -e RESTATE_TARGET_SERVICE=OrderService \
  -e RESTATE_TARGET_HANDLER=onEvent \
  ghcr.io/restatedev/ingress-integration-kafka:latest
```

## Configuration

Configuration comes from environment variables, optionally layered over a Kafka properties file
(env wins).

### Kafka consumer properties — `KAFKA_*`

Any `KAFKA_*` variable becomes a Kafka consumer property using the Confluent Docker naming
convention: lower-cased, then `___` → `-`, `__` → `_`, `_` → `.`.

| Env | Kafka property |
|-----|----------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `bootstrap.servers` (**required**) |
| `KAFKA_GROUP_ID` | `group.id` (**required**) |
| `KAFKA_AUTO_OFFSET_RESET` | `auto.offset.reset` (e.g. `earliest`) |
| `KAFKA_SASL_JAAS_CONFIG` | `sasl.jaas.config` |
| `KAFKA_SECURITY_PROTOCOL` | `security.protocol` |
| `KAFKA_MAX_POLL_RECORDS` | `max.poll.records` (set to `1` for strict 1:1 pull) |

`key.deserializer`, `value.deserializer` and `enable.auto.commit=false` are always managed by the
integration and cannot be overridden.

### Restate wiring & runtime (dedicated, not forwarded to Kafka)

| Env | Meaning |
|-----|---------|
| `KAFKA_TOPICS` | Comma-separated list of topics to subscribe to (**required**) |
| `RESTATE_INGRESS_URL` | Restate ingestion endpoint, e.g. `http://host:8080` (`https` ⇒ TLS) (**required**) |
| `RESTATE_TARGET_SERVICE` | Restate service to invoke (**required**) |
| `RESTATE_TARGET_HANDLER` | Restate handler to invoke (**required**) |
| `RESTATE_KAFKA_CONSUMER_INSTANCES` | Number of consumer instances deployed (one per event loop). Default `2 × CPUs`. Set ≤ the topics' partition count. |
| `CONFIG_FILE` | Path to a `.properties` file with base Kafka properties (env overrides it) |

The app runs on Vert.x's application launcher, so Vert.x itself is tunable via its `VERTX_*`
environment variables (e.g. event-loop pool size), and shutdown is graceful on SIGTERM/SIGINT.
Logging is Log4j2; override the bundled config with `LOG4J_CONFIGURATION_FILE=/path/log4j2.properties`
(or `-Dlog4j2.configurationFile=...`).

On missing/invalid required configuration the process exits with a clear message (exit code `2`).
A fatal misconfiguration reported by Restate (unknown service/handler) exits with code `1`; other
errors (server shutting down, transient) reconnect with backoff.

## Building & running locally

```bash
./gradlew build            # compile + tests
./gradlew installDist      # produces build/install/ingress-integration-kafka/
./gradlew run              # run under the Java 25 toolchain (provide the env vars above)
```

The container image is built with [Jib](https://github.com/GoogleContainerTools/jib) — no
Dockerfile or Docker daemon required:

```bash
./gradlew jib              # build and push to the registry (ghcr.io/restatedev/...)
./gradlew jibDockerBuild   # build into the local Docker/Podman daemon
./gradlew jibBuildTar      # build to build/jib-image.tar (fully offline)
```

Requires the Java 25 toolchain to run the built scripts (`./gradlew run` uses it automatically).

## Status

Proof of concept. The Restate server side of the ingestion protocol
([restatedev/restate#5024](https://github.com/restatedev/restate/pull/5024)) is still in
development; this consumer is tested against an in-process fake `IngestionSvc` server. See
`PLAN.md` for the design and roadmap.
