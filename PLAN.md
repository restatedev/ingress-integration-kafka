# Kafka Ingress Integration 2.0 — Implementation Plan

An external, opinionated Kafka consumer that streams records into Restate through the
new **Ingress Integration API** (`restate.ingestion.IngestionSvc`), replacing the
built-in librdkafka consumer.

## 1. The protocol (from restatedev/restate#5024)

`service IngestionSvc { rpc Ingest(stream Request) returns (stream Response); }`

- **1 HTTP/2 stream = 1 Restate "producer" = 1 `(topic, partition)`** in Kafka land.
- Client → server `Request` is a `oneof { Settings, Record }`.
  - `Settings` sets stream defaults (must be resent in full to change): `producer_id`
    (dedup key — **must be stable & unique per (cluster,topic,partition)**; empty disables
    dedup), `service`, `handler`, `headers`, `scope`, `limit_key`.
  - `Record`: `offset`, `key` (VO/Workflow key), `traceparent`/`tracestate`,
    per-record overrides (`service`/`handler`/`scope`/`limit_key`), `additional_headers`
    (merged over Settings headers), `payload` (bytes).
- Server → client `Response`:
  - `WindowUpdate{ increment }` — **pull-based flow control credits**. Client may send at
    most `increment` more records; `increment=0` doubles as a pure ack.
  - `Error{ kind, message }` — `SHUTTING_DOWN` (reconnect/backoff),
    `UNKNOWN_SERVICE`/`UNKNOWN_HANDLER` (fatal config error).
  - `last_committed` (optional uint64) — highest **durably committed** offset; drives the
    Kafka offset commit. 0-based, hence optional.

**Exactly-once**: Restate dedups by `(producer_id, offset)`; we only commit an offset back
to Kafka once Restate reports it in `last_committed`. Re-sent records after a restart are
deduped server-side.

## 2. Parity target (matches today's built-in consumer)

For the PoC we match the existing Restate Kafka experience: one consumer group subscribes to
one or more topics (`KAFKA_TOPICS`), and every record routes to a single
`service://<Service>/<handler>` target.

- Kafka record **key** (UTF-8 string) → `Record.key` (the VO/Workflow key).
- Kafka record **value** (bytes) → `Record.payload`.
- Kafka **offset** → `Record.offset`.
- `additional_headers`: `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp`,
  `kafka.key`. `Settings.headers` defaults to empty (no subscription concept anymore).
- **No `content-type`** header is set for now.
- `traceparent`/`tracestate` copied from Kafka record headers if present.
- `scope` / `limit_key` left unset for the PoC (optional in the protocol).
- `producer_id = "${group.id}/${topic}/${partition}"`. These are the stable per-stream bits;
  the dedup coordinate that pairs with it is the per-record `offset`. Leading with `group.id`
  keeps distinct consumer groups independently deduped while surviving rebalances/restarts
  within a group. There is **no** subscription id (subscriptions are no longer a concept).

## 3. Stack

- Java toolchain **25**, Kotlin **2.3.x** + coroutines (`vertx-lang-kotlin-coroutines`).
- **Vert.x 5.1.5**: `vertx-core`, `vertx-kafka-client`, `vertx-grpc-client` (protobuf
  (de)serialization lives in the transitive `vertx-grpc-common`); codegen via
  `vertx-grpc-protoc-plugin2` (emits the `*Client`/`*Service` stubs) driven by the
  `com.google.protobuf` Gradle plugin, on protobuf **4.29.3**.
- **Testing**: JUnit **6** + AssertJ; Vert.x driven via Kotlin coroutines.
- **Concurrency (Approach B — idiomatic Vert.x)**: deploy `ConsumerVerticle` with **N
  instances** (via a manual deploy loop → one per event loop). N =
  `RESTATE_KAFKA_CONSUMER_INSTANCES` env (default `2 × availableProcessors`, clamped ≥ 1);
  we set both the instance count and `VertxOptions.eventLoopPoolSize` to N so it's 1 consumer
  per event loop. (Core Vert.x has no built-in env for pool size — we read our own.) Each instance owns its own
  `KafkaConsumer` in the same `group.id`; Kafka distributes partitions across the instances
  and rebalances automatically. Each instance is fully **single-context** → lock-free,
  per-partition offset order preserved, no cross-context hand-offs. Useful parallelism is
  capped at `min(instances, partitions)`; extra instances idle. (Manual deploy loop avoids
  the Vert.x 5.0.x `setInstances`+`VIRTUAL_THREAD` regression, issue #5924.)

## 4. Pull-based bridge (the crux)

Within each `ConsumerVerticle` instance (single context): `enable.auto.commit=false`,
subscribe to the topic. For each partition this instance is assigned, keep
`{ credits, paused, buffer, lastSentOffset }` and one gRPC `Ingest` stream:

1. On assignment: open stream, send `Settings`, **pause the partition** (credits=0).
2. On `WindowUpdate(inc)`: `credits += inc`; drain `buffer` first (send up to credits); if
   `credits>0` and partition was paused → `resume(tp)` so Kafka fetches more.
3. On record: if `credits>0` → send `Record`, `credits--`, track offset; if `credits==0` →
   `buffer` it and `pause(tp)` (poll batches can overrun the exact credit). Honor gRPC
   `writeQueueFull`/`drainHandler` as a secondary guard.
4. On `last_committed=o`: `consumer.commit({tp: o+1})`.
5. On `Error`: fatal → log + non-zero exit; `SHUTTING_DOWN` → close stream + backoff reconnect.
6. On revoke: half-close (`end()`) the stream, drop state.

Net effect: **Restate's window credits gate Kafka fetching** → true pull semantics.

## 5. Config

Two sources, env overrides file:
- **Kafka consumer props**: `KAFKA_*` env → property name via the Confluent convention
  (`_`→`.`, `__`→`_`, `___`→`-`, lowercased); e.g. `KAFKA_BOOTSTRAP_SERVERS`→`bootstrap.servers`.
  Also a `--config <file>` / `CONFIG_FILE` properties file, overlaid by env. Key/value
  deserializers are forced (key=String, value=ByteArray).
- **Restate wiring** (dedicated, not forwarded to Kafka): `RESTATE_INGRESS_URL`
  (e.g. `http://host:port`, `https` ⇒ TLS), `RESTATE_TARGET_SERVICE`, `RESTATE_TARGET_HANDLER`,
  `KAFKA_TOPICS` (comma-separated, ≥1; special-cased so it isn't forwarded as a Kafka
  consumer property). One consumer group subscribes to all of them; every record routes to
  the single target (per-topic → per-target routing is a later extension). `producer_id` is
  derived from `group.id`+topic+partition, so there is no separate subscription/producer-id env.
- **Parallelism**: `RESTATE_KAFKA_CONSUMER_INSTANCES` (default `2 × availableProcessors`,
  clamped ≥ 1) → number of consumer instances = event-loop pool size. Not `KAFKA_`-prefixed
  (that namespace maps to Kafka consumer props). Guidance: set ≤ the topic's partition count.
- Fail fast with a clear message on missing/invalid required config.

## 6. Project structure (single Gradle module)

```
src/main/proto/ingestion_svc.proto        # vendored from PR #5024
src/main/kotlin/dev/restate/integration/kafka/
  Main.kt                # CLI entry: load config -> deploy N ConsumerVerticle instances -> shutdown hook
  config/AppConfig.kt    # env + properties parsing/validation
  IngestionClient.kt     # GrpcClient wrapper: open Ingest stream, Settings/Record, parse Response
  ConsumerVerticle.kt    # CoroutineVerticle (deployed N times, one per event loop): KafkaConsumer + rebalance wiring
  PartitionStream.kt     # per-partition credits/buffer/commit + its gRPC stream (owned by the instance)
  RecordMapper.kt        # KafkaConsumerRecord -> Record (headers, key, tracing)
src/test/kotlin/...      # unit + integration tests
Dockerfile
README.md
```

## 7. Phases

- **P1 — Scaffolding & build**: `build.gradle.kts` (Vert.x BOM, coroutines, protobuf+gRPC
  codegen), vendor the proto, confirm `./gradlew build` generates stubs and compiles a hello
  verticle.
- **P2 — Config**: `AppConfig` (env/properties, Confluent mapping, validation) + unit tests.
- **P3 — Ingestion gRPC client**: `IngestionClient` + `RecordMapper`; unit-tested against an
  in-process fake `IngestionSvc` server (grants windows, echoes `last_committed`, can inject
  errors). *(needed because the real server side is WIP)*
- **P4 — Kafka bridge**: `ConsumerVerticle` + `PartitionStream`, rebalance, credit-driven
  pause/resume, commit-on-`last_committed`.
- **P5 — Wiring & lifecycle**: `Main`, graceful shutdown, structured logging, error handling.
- **P6 — Image + README**: container image built with Jib (base `temurin:25-jre`, no Dockerfile
  / daemon), env-var contract documented, quickstart.

## 8. Decisions

- **Single Gradle module** (proto + ingestion client + kafka app together); split later if the
  ingestion client is published as a standalone artifact.
- **Fake-server unit tests only** for the PoC (in-process `IngestionSvc` fake). Real-Kafka
  Testcontainers e2e is deferred.
