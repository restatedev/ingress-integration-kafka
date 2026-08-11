# Restate Kafka ingress integration

A standalone service that turns Kafka records into [Restate](https://restate.dev) invocations. Run
it as a container next to your Restate deployment.

## What it does

- **Connects Kafka to Restate with no glue code** — each record on your topics triggers an
  invocation of a handler you choose.
- **Exactly-once processing** — every record is handled once, with no duplicates and no loss, even
  across restarts, redeploys and Kafka rebalances.
- **Keeps your ordering** — records from a partition are delivered in order.
- **Scales with your load** — run more replicas and throughput scales with your Kafka partitions.
- **Runs and scales independently of Restate** — deploy, upgrade and size it on its own, without
  touching your Restate server.
- **Resilient by default** — it reconnects through transient outages with backoff, and fails fast on
  real misconfiguration so your orchestrator restarts it cleanly.
- **Works with your Kafka** — any consumer setting, including SASL/TLS auth, passes straight through.
- **Observable** — exposes Prometheus metrics for the Kafka client, throughput, retries and errors out
  of the box (see [Metrics](#metrics)).

## Quickstart

```bash
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e KAFKA_GROUP_ID=orders-to-restate \
  -e KAFKA_TOPICS=orders,payments \
  -e RESTATE_INGRESS_URL=http://restate:8080 \
  -e RESTATE_RECORD_MAPPER_SERVICE=OrderService \
  -e RESTATE_RECORD_MAPPER_HANDLER=onKafkaEvent \
  ghcr.io/restatedev/ingress-integration-kafka:latest
```

## Configuration

Configure via environment variables or a `.properties` file (configured via `CONFIG_FILE` env).

### 1. Kafka, topics, parallelism & the Restate client

**Required**

| Env                       | Property key          | Description                                                         |
|---------------------------|-----------------------|---------------------------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `bootstrap.servers`   | Kafka bootstrap servers, e.g. `broker1:9092,broker2:9092`.          |
| `KAFKA_GROUP_ID`          | `group.id`            | Kafka consumer group id (also the prefix of the dedup producer id). |
| `KAFKA_TOPICS`            | `topics`              | Comma-separated topics to subscribe to, e.g. `orders,payments`.     |
| `RESTATE_INGRESS_URL`     | `restate.ingress.url` | Restate ingestion endpoint, `http://host:port` or `https://host`.   |

**Optional**

| Env                                   | Property key                          | Default              | Description                                                        |
|---------------------------------------|---------------------------------------|----------------------|--------------------------------------------------------------------|
| `CONFIG_FILE`                         | – (env only)                          | –                    | Path to a `.properties` file of base config (env wins).            |
| `RESTATE_AUTH_TOKEN`                  | `restate.auth.token`                  | –                    | Bearer token for the Restate ingress (for Cloud and BYOC).         |
| `RESTATE_KAFKA_CONSUMER_INSTANCES`    | `restate.kafka.consumer.instances`    | 2 × CPU cores        | Consumer instances per process (partition parallelism).            |
| `RESTATE_RETRY_INITIAL_INTERVAL_MS`   | `restate.retry.initial.interval.ms`   | `500`                | Initial reconnect backoff after a dropped ingestion stream.        |
| `RESTATE_RETRY_MAX_INTERVAL_MS`       | `restate.retry.max.interval.ms`       | `30000`              | Maximum reconnect backoff.                                         |
| `RESTATE_RETRY_EXPONENTIATION_FACTOR` | `restate.retry.exponentiation.factor` | `2.0`                | Backoff growth factor (≥ 1.0).                                     |
| `RESTATE_RETRY_MAX_ATTEMPTS`          | `restate.retry.max.attempts`          | `15` (≈ 5 min)       | Give up (and exit) after this many consecutive failed attempts; unset to retry indefinitely. |
| `RESTATE_RECORD_MAPPER_CLASS`         | `restate.record.mapper.class`         | `StaticRecordMapper` | The `RecordMapper` class to load (see below).                      |
| `RESTATE_METRICS_ENABLED`             | `restate.metrics.enabled`             | `true`               | Expose Prometheus metrics on `/metrics` (see [Metrics](#metrics)). |
| `RESTATE_METRICS_PORT`                | `restate.metrics.port`                | `9464`               | Port for the `/metrics` scrape endpoint.                           |

**Extra Kafka consumer settings.** Any other `KAFKA_*` variable is forwarded to the Kafka consumer
using the Confluent naming convention — lowercased, with underscore runs mapped to separators:
single `_` → `.`, double `__` → `_`, triple `___` → `-`. For example `KAFKA_SECURITY_PROTOCOL` →
`security.protocol`.

A SASL/TLS setup:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";
```

If your broker needs an auth plugin that isn't on the classpath (e.g. a cloud OAuth login handler),
see [Adding an auth provider](#adding-an-auth-provider-or-any-extra-library).

Commit mode and key/value deserializers are automatically configured.

Logging is Log4j2; point it at your own config with `-Dlog4j2.configurationFile=/path/log4j2.xml`.

### 2. Record mapper

The record mapper turns each Kafka record into a Restate invocation (target service/handler, key,
payload, …). Pick the implementation with `RESTATE_RECORD_MAPPER_CLASS`, then configure it under `RESTATE_RECORD_MAPPER_*`:

#### Static record mapper (default)

Every record goes to the same service/handler.
The Kafka key (when available) becomes the VO/Workflow key and the value becomes the payload.

| Env                                    | Property key                           | Required | Description                                                                                |
|----------------------------------------|----------------------------------------|----------|-------------------------------------------------------------------------------------------|
| `RESTATE_RECORD_MAPPER_SERVICE`        | `restate.record.mapper.service`        | yes      | Restate service to invoke.                                                                 |
| `RESTATE_RECORD_MAPPER_HANDLER`        | `restate.record.mapper.handler`        | yes      | Handler on that service to invoke.                                                         |
| `RESTATE_RECORD_MAPPER_KAFKA_METADATA` | `restate.record.mapper.kafka.metadata` | no       | Attach the `kafka.topic`/`kafka.partition`/`kafka.offset`/`kafka.timestamp` headers (default `true`). |

| Kafka                                        | Restate `Record`                                                                                       |
|----------------------------------------------|--------------------------------------------------------------------------------------------------------|
| key (UTF-8)                                  | `key` — the Virtual Object / Workflow key (required by Restate for VO/Workflow targets)                |
| value (bytes)                                | `payload` (a null value / tombstone → empty payload)                                                   |
| topic / partition / offset / timestamp       | headers `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp` (unless `kafka.metadata=false`) |
| key                                          | header `kafka.key`                                                                                     |
| `traceparent` / `tracestate` headers         | propagated to `traceparent` / `tracestate`                                                             |

#### JSON dynamic-target mapper

Parses each record value as JSON and derives the target (and optional fields) *per record*. Enable
it with:

```properties
restate.record.mapper.class=dev.restate.integration.kafka.mapper.JsonDynamicTargetRecordMapper
```

Each field is configured by setting **exactly one** of three sub-keys:

| Sub-key                   | Source                                                                                          |
|---------------------------|-------------------------------------------------------------------------------------------------|
| `<field>.value=<literal>` | a static value                                                                                  |
| `<field>.fromkey=true`    | the Kafka record key                                                                            |
| `<field>.pointer=/a/b`    | a value read from the JSON payload via a [JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901) |

Fields: `service` (required), `handler` (required), `key`, `idempotencykey`, `scope`, `limitkey`.

The payload is the record JSON value, and the `kafka.*` metadata headers (toggled by
`restate.record.mapper.kafka.metadata`, default `true`) and trace context propagate as above. 

Example:

```properties
restate.record.mapper.class=dev.restate.integration.kafka.mapper.JsonDynamicTargetRecordMapper
# Send all records to the Order
restate.record.mapper.service.value=Order
# Use the "type" field to determine the handler
restate.record.mapper.handler.pointer=/type
# Use the "customerId" field as the virtual object key
restate.record.mapper.key.pointer=/customerId
# Use the "eventId" field as the idempotency key
restate.record.mapper.idempotencykey.pointer=/eventId
```

## Metrics

Prometheus metrics are exposed by default on `http://<host>:9464/metrics` (Prometheus text format).
Turn them off with `RESTATE_METRICS_ENABLED=false`, or move the port with `RESTATE_METRICS_PORT`.

```bash
curl localhost:9464/metrics
```

Four families are published against one registry:

| Prefix                             | What                                                     |
|------------------------------------|----------------------------------------------------------|
| `kafka_consumer_*`                 | Native Kafka consumer metrics.                           |
| `restate_kafka_*`                  | Ingestion-path metrics.                                  |
| `vertx_*`                          | Vert.x internals.                                        |
| `jvm_*` / `system_*` / `process_*` | Standard JVM & process metrics (heap, GC, threads, CPU). |

## Adding an auth provider (or any extra library)

Some brokers need an auth plugin that isn't on the image's classpath, e.g. Google Cloud Managed
Service for Apache Kafka uses SASL/OAUTHBEARER with a login-callback class shipped in
`managed-kafka-auth-login-handler`. You can layer it on without rebuilding from source.

Example:

```dockerfile
# 1. Resolve the auth library JAR + its transitive deps into a folder.
FROM maven:3-eclipse-temurin-21 AS auth
WORKDIR /build
COPY <<'EOF' pom.xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>x</groupId><artifactId>x</artifactId><version>1</version>
  <dependencies>
    <dependency>
      <groupId>com.google.cloud.hosted.kafka</groupId>
      <artifactId>managed-kafka-auth-login-handler</artifactId>
      <version>1.0.6</version>
      <!-- kafka-clients is already in the image -->
      <exclusions>
        <exclusion>
          <groupId>org.apache.kafka</groupId>
          <artifactId>kafka-clients</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
  </dependencies>
</project>
EOF
RUN mvn -q -DincludeScope=runtime -DoutputDirectory=/libs dependency:copy-dependencies

# 2. Layer them onto the ingress image. `/app/extra-libs/` is already on the classpath, so you
#    just drop the jars in — no need to touch the jib-classpath-file.
FROM ghcr.io/restatedev/ingress-integration-kafka:latest
COPY --from=auth /libs/ /app/extra-libs/
```

Then configure the handler like any other Kafka setting (env or properties file):

```properties
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
```

The handler authenticates with Application Default Credentials, so run the container with a GCP
identity, Workload Identity on GKE, or mount a service-account key and set
`GOOGLE_APPLICATION_CREDENTIALS`.

The same pattern works for any auth plugin (AWS MSK IAM, Azure Event Hubs, …): resolve its jars,
drop them in `/app/extra-libs/`, and set the matching `security.protocol` / `sasl.*` config.

## Building the image

Built with [Jib](https://github.com/GoogleContainerTools/jib) — no Dockerfile or Docker daemon
required:

```bash
./gradlew jibDockerBuild   # build into the local Docker/Podman daemon
./gradlew jibBuildTar      # build to build/jib-image.tar
./gradlew jib              # build and push to the configured registry
```
