# Configuration

Everything you can tune on the Restate Kafka ingress integration, in one place.

Configure via environment variables or a `.properties` file (pointed at with the `CONFIG_FILE` env
var). When both set the same key, the environment variable wins.

`CONFIG_FILE` accepts a comma-separated list of files (e.g. `base.properties,overrides.properties`);
they are merged left to right, so a later file overrides an earlier one on any shared key (and the
environment still wins over all of them).

## Kafka & Restate client configuration

| Env                                   | Property key                          | Required | Default              | Description                                                                                  |
|---------------------------------------|---------------------------------------|----------|----------------------|----------------------------------------------------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`             | `bootstrap.servers`                   | yes      | –                    | Kafka bootstrap servers, e.g. `broker1:9092,broker2:9092`.                                   |
| `KAFKA_GROUP_ID`                      | `group.id`                            | yes      | –                    | Kafka consumer group id (also the prefix of the dedup producer id).                          |
| `KAFKA_TOPICS`                        | `topics`                              | yes      | –                    | Comma-separated topics to subscribe to, e.g. `orders,payments`.                              |
| `RESTATE_INGRESS_URL`                 | `restate.ingress.url`                 | yes      | –                    | Restate ingestion endpoint, `http://host:port` or `https://host`.                            |
| `CONFIG_FILE`                         | – (env only)                          | no       | –                    | Comma-separated `.properties` file(s) of base config; later files override earlier, env wins. |
| `RESTATE_AUTH_TOKEN`                  | `restate.auth.token`                  | no       | –                    | Bearer token for the Restate ingress (for Cloud and BYOC).                                   |
| `RESTATE_KAFKA_CONSUMER_INSTANCES`    | `restate.kafka.consumer.instances`    | no       | 2 × CPU cores        | Consumer instances per process (partition parallelism).                                      |
| `RESTATE_RETRY_INITIAL_INTERVAL_MS`   | `restate.retry.initial.interval.ms`   | no       | `500`                | Initial reconnect backoff after a dropped ingestion stream.                                  |
| `RESTATE_RETRY_MAX_INTERVAL_MS`       | `restate.retry.max.interval.ms`       | no       | `30000`              | Maximum reconnect backoff.                                                                   |
| `RESTATE_RETRY_EXPONENTIATION_FACTOR` | `restate.retry.exponentiation.factor` | no       | `2.0`                | Backoff growth factor (≥ 1.0).                                                               |
| `RESTATE_RETRY_MAX_ATTEMPTS`          | `restate.retry.max.attempts`          | no       | `15` (≈ 5 min)       | Give up (and exit) after this many consecutive failed attempts; unset to retry indefinitely. |
| `RESTATE_RECORD_MAPPER_CLASS`         | `restate.record.mapper.class`         | no       | `StaticRecordMapper` | The `RecordMapper` class to load (see [Record mapper](#record-mapper)).                      |
| `RESTATE_METRICS_ENABLED`             | `restate.metrics.enabled`             | no       | `true`               | Expose Prometheus metrics on `/metrics` (see [Metrics](#metrics)).                           |
| `RESTATE_METRICS_PORT`                | `restate.metrics.port`                | no       | `9464`               | Port for the `/metrics` scrape endpoint.                                                     |

### Extra Kafka consumer settings

Any other `KAFKA_*` variable is forwarded to the Kafka consumer using the Confluent naming
convention — lowercased, with underscore runs mapped to separators: single `_` → `.`, double `__` →
`_`, triple `___` → `-`. For example `KAFKA_SECURITY_PROTOCOL` → `security.protocol`.

A SASL/TLS setup:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";
```

If your broker needs an auth plugin that isn't on the classpath (e.g. a cloud OAuth login handler),
see [Adding an auth provider](#adding-an-auth-provider-or-any-extra-library).

Commit mode and key/value deserializers are automatically configured.

## Record mapper

The record mapper turns each Kafka record into a Restate invocation (target service/handler, key,
payload, …). Pick the implementation with `RESTATE_RECORD_MAPPER_CLASS` or `restate.record.mapper.class`, then configure it.

### Static record mapper (default)

```.dotenv
RESTATE_RECORD_MAPPER_CLASS=dev.restate.integration.kafka.mapper.StaticRecordMapper
```

Every record goes to the same service/handler. The Kafka record key (when available) becomes the VO/Workflow key and the record value becomes the invocation payload.

**Configuration**:

| Env                                    | Property key                           | Required | Description                                                                                           |
|----------------------------------------|----------------------------------------|----------|-------------------------------------------------------------------------------------------------------|
| `RESTATE_RECORD_MAPPER_SERVICE`        | `restate.record.mapper.service`        | yes      | Restate service to invoke.                                                                            |
| `RESTATE_RECORD_MAPPER_HANDLER`        | `restate.record.mapper.handler`        | yes      | Handler on that service to invoke.                                                                    |
| `RESTATE_RECORD_MAPPER_KAFKA_METADATA` | `restate.record.mapper.kafka.metadata` | no       | Attach the `kafka.topic`/`kafka.partition`/`kafka.offset`/`kafka.timestamp` headers (default `true`). |

**Mapping reference**:

| Kafka                                  | Restate Invocation                                                                         |
|----------------------------------------|--------------------------------------------------------------------------------------------|
| key (UTF-8)                            | `key` — the Virtual Object / Workflow key (required by Restate for VO/Workflow targets)    |
| value (bytes)                          | `payload` (a null value / tombstone → empty payload)                                       |
| topic / partition / offset / timestamp | headers `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp` (when enabled) |
| key                                    | header `kafka.key`                                                                         |
| `traceparent` / `tracestate` headers   | propagated to `traceparent` / `tracestate`                                                 |

### JSON dynamic-target mapper

```.dotenv
RESTATE_RECORD_MAPPER_CLASS=dev.restate.integration.kafka.mapper.JsonDynamicTargetRecordMapper
```

Parses each record value as JSON and derives the target (and optional fields) *per record*.

**Configuration**:

Each field is configured by setting **exactly one** of three sub-keys:

| Sub-key                   | Source                                                                                          |
|---------------------------|-------------------------------------------------------------------------------------------------|
| `<field>.value=<literal>` | a static value                                                                                  |
| `<field>.fromkey=true`    | the Kafka record key                                                                            |
| `<field>.pointer=/a/b`    | a value read from the JSON payload via a [JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901) |

Configurable fields: `service` (required), `handler` (required), `key`, `idempotencykey`, `scope`, `limitkey`.

The payload is always the record JSON value, and the `kafka.*` metadata headers (toggled by
`restate.record.mapper.kafka.metadata`, default `true`) and trace context propagate as for the static mapper.

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

## Observability

### Metrics

Prometheus metrics are exposed by default on `http://<host>:9464/metrics` (Prometheus text format).
Turn them off with `RESTATE_METRICS_ENABLED=false`, or move the port with `RESTATE_METRICS_PORT`.

```bash
curl localhost:9464/metrics
```

Four metrics families are published:

| Prefix                             | What                                                     |
|------------------------------------|----------------------------------------------------------|
| `kafka_consumer_*`                 | Native Kafka consumer metrics.                           |
| `restate_kafka_*`                  | Ingestion-path metrics.                                  |
| `vertx_*`                          | Vert.x internals.                                        |
| `jvm_*` / `system_*` / `process_*` | Standard JVM & process metrics (heap, GC, threads, CPU). |

### Logging

Logging is Log4j2; point it at your own config with `-Dlog4j2.configurationFile=/path/log4j2.xml`.

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

# 2. Layer them onto the ingress image. `/app/extra-libs/` is already on the classpath
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

The same pattern works for any auth plugin (AWS MSK IAM, Azure Event Hubs, …): resolve its jars,
drop them in `/app/extra-libs/`, and set the matching `security.protocol` / `sasl.*` config.
