# CLAUDE.md

Guidance for working in this repo. It is written for people (and agents) who want to **customize**
this service — most often by writing a custom `RecordMapper` (see [Customizing: write a
RecordMapper](#customizing-write-a-recordmapper), which is where you almost certainly want to go).

## What this is

A standalone JVM service (Kotlin + Vert.x 5) that consumes Kafka records and turns each one into a
[Restate](https://restate.dev) invocation over a resilient, exactly-once gRPC ingestion stream. It
ships as a container built with Jib. User-facing docs live in [`README.md`](README.md) and
[`CONFIGURATION.md`](CONFIGURATION.md) — keep those in sync when you change behavior.

## Commands

```bash
./gradlew build            # compile + test + spotless check
./gradlew test             # run tests (JUnit 5). Unit tests are fast; the e2e test uses Testcontainers
./gradlew test --tests 'dev.restate.integration.kafka.mapper.*'   # just the mapper tests
./gradlew spotlessApply    # format (ktfmt) — CI fails on unformatted code, run before committing
./gradlew run              # run locally (needs Kafka + Restate reachable; see env in README)
./gradlew jibDockerBuild   # build the container image into the local Docker/Podman daemon
docker compose up -d       # bring up the full local stack (Kafka + Restate + this service)
```

- JDK 25 toolchain (`kotlin { jvmToolchain(25) }`). The e2e test in particular needs a real JDK 25;
  if `KafkaToRestateE2ETest` misbehaves, that is usually the environment (or the moving `restate:main`
  image), not a code regression.
- The gRPC types (`dev.restate.ingestion.v1.*`) are **generated** from
  [`src/main/proto/ingestion_svc.proto`](src/main/proto/ingestion_svc.proto) at build time. Read that
  proto to understand the wire contract — it is the source of truth for the ingestion protocol.

## Architecture

The runtime is a short pipeline. Data flows **Kafka record → RecordMapper → ProducerSession → Restate**.

```
Main.kt
  └─ loads AppConfig (env + optional CONFIG_FILE properties), then launches N ConsumerVerticle instances
       │
ConsumerVerticle (one KafkaConsumer per Vert.x event loop; N of them)
  ├─ on partition assigned  → opens one ProducerSession per (topic, partition)
  ├─ on each record         → recordMapper.toInvocation(record) → session.offer(invocation)
  │                            (a null mapping filters the record out)
  └─ on ack from Restate    → commits the Kafka offset
       │
ProducerSession (client/ProducerSession.kt) — one resilient gRPC `Ingest` stream per partition
  ├─ streams invocations to Restate with flow control + backpressure (pauses/resumes the partition)
  ├─ on ack (last_committed) → tells the consumer to commit that Kafka offset
  └─ on stream drop          → reconnects with backoff, rewinds to last committed offset, resends
```

Key files:

| File | Role |
|------|------|
| `kafka/Main.kt` | Entry point; wires Vert.x, metrics, deploys `ConsumerVerticle` × `consumerInstances`. |
| `kafka/AppConfig.kt` | Merges properties file + env into Kafka config + `RestateConfig` + the `RecordMapper`. |
| `kafka/RestateConfig.kt` | `restate.*` options via Kafka's `ConfigDef`; instantiates the mapper "the Kafka way". |
| `kafka/ConsumerVerticle.kt` | The consumer loop; owns the `KafkaConsumer` and its `ProducerSession`s. |
| **`kafka/RecordMapper.kt`** | **The extension point.** Record → invocation. This is what you customize. |
| `kafka/mapper/*` | The two built-in mappers (`StaticRecordMapper`, `JsonDynamicTargetRecordMapper`). |
| `client/*` | The resilient ingestion client: `ProducerSession`, `IntegrationClient`, `InvocationStream`. |

### Concurrency model (important before you touch anything)

Each `ConsumerVerticle` instance runs entirely on **one** Vert.x event-loop thread: its consumer,
all its `ProducerSession`s, and their gRPC streams share that single context, so there is **no
locking**. Deploy more instances (`RESTATE_KAFKA_CONSUMER_INSTANCES`) to use more event loops; Kafka
spreads partitions across them and rebalances automatically. **Do not block the event loop** and do
not introduce shared mutable state across instances. A `RecordMapper.toInvocation` call runs on this
thread and must be non-blocking and reasonably fast.

### Exactly-once / offset invariant

Deduplication is offset-based, keyed by a stable `producer_id` (here
`"<groupId>/<topic>/<partition>"`, set in `ConsumerVerticle.openPartition`). The server tracks the
highest committed offset per producer and silently drops anything not strictly above it. This is why
Kafka offsets are committed **only after** Restate acks (`enable.auto.commit` is forced to `false` in
`AppConfig`), and why a reconnect rewinds and resends. **Every invocation must carry a monotonically
increasing `offset`.** See the long comments in `ingestion_svc.proto` for the exact semantics.

## Customizing: write a RecordMapper

**99% of customization is a `RecordMapper`.** It is the one interface you implement to control how a
Kafka record becomes a Restate invocation — the target service/handler, the key, the payload,
headers, filtering, and even how the raw Kafka bytes are deserialized.

### The interface

[`kafka/RecordMapper.kt`](src/main/kotlin/dev/restate/integration/kafka/RecordMapper.kt):

```kotlin
interface RecordMapper<K, V> {
  val keyDeserializer: Class<out Deserializer<K>>     // how Kafka bytes → K
  val valueDeserializer: Class<out Deserializer<V>>   // how Kafka bytes → V
  fun initialDefaults(): StreamDefaults               // stream-wide defaults, set once per partition
  fun toInvocation(record: ConsumerRecord<K, V>): IngestionInvocation?  // per record; null = filter out
}
```

Four things a mapper controls:

1. **Deserialization.** The mapper *owns* the Kafka key/value deserializers — `AppConfig.load()`
   copies `keyDeserializer`/`valueDeserializer` into the Kafka consumer config, so `<K, V>` are
   whatever your deserializers produce. `StaticRecordMapper` uses `String`/`ByteArray`;
   `JsonDynamicTargetRecordMapper` uses `String`/`JsonNode` (with its own `JsonNodeDeserializer`).
2. **Defaults vs. per-record (`initialDefaults` vs. `toInvocation`).** `initialDefaults()` returns a
   `StreamDefaults` (typealias for the `IngestionDefaults` proto) applied to every record on that
   partition's stream. `toInvocation()` sets only what varies per record; a field set on the
   invocation overrides the default. **Put anything constant in `initialDefaults()`** — it is set once
   and is cheaper and clearer than repeating it on every record. (`headers` are the exception: an
   invocation's `additional_headers` are *appended* to the defaults' headers, not replaced.)
3. **The mapping itself.** Build an `IngestionInvocation` — see fields below.
4. **Filtering.** Return `null` from `toInvocation` to drop a record (it is skipped, not sent).

### `IngestionInvocation` fields you can set

(From `ingestion_svc.proto`; build with `IngestionInvocation.newBuilder()`.)

| Field | Notes |
|-------|-------|
| `offset` | **Required. Set it to `record.offset()`.** Drives dedup/exactly-once; must increase monotonically per partition. |
| `service` / `handler` | Target. Required (here, or via `initialDefaults()`). |
| `key` | The Virtual Object / Workflow key. **Required by Restate when the target is a VO or Workflow.** |
| `payload` | `bytes`. A null Kafka value (tombstone) → empty payload. |
| `idempotencyKey` | Optional idempotency key for the invocation. |
| `scope` / `limitKey` | Optional concurrency scope / limiting key. |
| `additionalHeaders` | Appended to the stream defaults' headers; delivered as request headers. |
| `traceparent` / `tracestate` | W3C trace context; propagate from Kafka headers if present. |
| `delayMs` **or** `invokeTimeTsMs` | Optional scheduling (mutually exclusive). |

### How a mapper is selected and configured

Loaded "the Kafka way" in `RestateConfig.recordMapper()`:

- `restate.record.mapper.class` / `RESTATE_RECORD_MAPPER_CLASS` names the implementation
  (default `StaticRecordMapper`). It is instantiated via its **no-arg constructor** — your mapper
  must have one.
- If the mapper implements `org.apache.kafka.common.Configurable`, it receives everything under
  `restate.record.mapper.*` (prefix stripped) via `configure(configs)`. Env vars map to property
  keys by the Confluent convention (`RESTATE_RECORD_MAPPER_SERVICE` → `restate.record.mapper.service`
  → your `configure` sees `"service"`). Validate required keys in `configure` and **throw** on bad
  config — the mapper is built eagerly at startup (`AppConfig.load`), so a bad config fails fast
  rather than at first record.

### A minimal custom mapper

```kotlin
package dev.restate.integration.kafka.mapper

class MyMapper : RecordMapper<String, ByteArray>, Configurable {
  override val keyDeserializer = StringDeserializer::class.java
  override val valueDeserializer = ByteArrayDeserializer::class.java

  private lateinit var service: String

  override fun configure(configs: MutableMap<String, *>) {
    service = (configs["service"] as? String)?.takeIf { it.isNotBlank() }
      ?: throw IllegalArgumentException("restate.record.mapper.service is required")
  }

  override fun initialDefaults(): StreamDefaults =
    StreamDefaults.newBuilder().setService(service).setHandler("onEvent").build()

  override fun toInvocation(record: ConsumerRecord<String, ByteArray>): IngestionInvocation? {
    if (record.value() == null) return null                 // filter tombstones, for example
    return IngestionInvocation.newBuilder()
      .setOffset(record.offset())                           // never forget this
      .apply { record.key()?.let { key = it } }             // VO/Workflow key
      .setPayload(ByteString.copyFrom(record.value()))
      .build()
  }
}
```

The two shipped mappers are the reference implementations — read them before writing your own:

- [`mapper/StaticRecordMapper.kt`](src/main/kotlin/dev/restate/integration/kafka/mapper/StaticRecordMapper.kt)
  — same target for every record; the simplest possible mapper.
- [`mapper/JsonDynamicTargetRecordMapper.kt`](src/main/kotlin/dev/restate/integration/kafka/mapper/JsonDynamicTargetRecordMapper.kt)
  — parses JSON and derives target/key/etc. per record via static value, Kafka key, or JSON pointer.
  A good template for anything data-driven.
- Shared `kafka.*` header handling lives in
  [`mapper/metadata.kt`](src/main/kotlin/dev/restate/integration/kafka/mapper/metadata.kt).

### Testing a mapper

Mappers are plain, synchronous, and trivially unit-testable — no Vert.x, no Kafka broker. Construct a
`ConsumerRecord` directly and assert on the resulting `IngestionInvocation`. Copy the pattern in
[`StaticRecordMapperTest`](src/test/kotlin/dev/restate/integration/kafka/mapper/StaticRecordMapperTest.kt)
or `JsonDynamicTargetRecordMapperTest`. Always add a test for a new mapper.

### Deploying your mapper (two options)

1. **In-tree:** add the class under `src/main/kotlin/dev/restate/integration/kafka/mapper/`, rebuild
   the image (`./gradlew jibDockerBuild`), and set `RESTATE_RECORD_MAPPER_CLASS` to it.
2. **Sideloaded (no source change):** build your mapper into its own jar and drop it (plus any deps)
   into `/app/extra-libs/` in a downstream image layer — that directory is already on the classpath
   (Jib `extraClasspath` in `build.gradle.kts`). Same mechanism as adding a Kafka auth plugin; see
   [Adding an auth provider](CONFIGURATION.md#adding-an-auth-provider-or-any-extra-library).

## Configuration system (how config reaches your code)

`AppConfig.load()` builds config from an optional `.properties` file (`CONFIG_FILE`, comma-separated,
later wins) overlaid by environment variables (**env always wins**). Keys split three ways:

- `KAFKA_*` env / any non-`restate.` file key → **Kafka consumer passthrough** (Confluent
  underscore→separator mapping). `key.deserializer`, `value.deserializer` and
  `enable.auto.commit=false` are forced by the app, not user-settable.
- `RESTATE_*` env / `restate.*` file keys → **`RestateConfig`** (typed via Kafka `ConfigDef`).
- `restate.record.mapper.*` → handed to your mapper's `configure()`.

The full option list is documented in [`CONFIGURATION.md`](CONFIGURATION.md) — update it when you add
config. See [`AppConfigTest`](src/test/kotlin/dev/restate/integration/kafka/AppConfigTest.kt) for the
merge/precedence rules.

## Conventions

- Kotlin, formatted with **ktfmt** via Spotless; run `./gradlew spotlessApply` before committing.
- Match the surrounding style: rich KDoc on the interesting classes, terse elsewhere.
- Keep `README.md` and `CONFIGURATION.md` in sync with behavior/config changes.
- Don't block the Vert.x event loop; keep per-record work (mappers) cheap and non-blocking.
