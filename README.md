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

## Quickstart

```bash
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=broker:9092 \
  -e KAFKA_GROUP_ID=orders-to-restate \
  -e KAFKA_TOPICS=orders,payments \
  -e RESTATE_INGRESS_URL=http://restate:8080 \
  -e RESTATE_TARGET_SERVICE=OrderService \
  -e RESTATE_TARGET_HANDLER=onKafkaEvent \
  ghcr.io/restatedev/ingress-integration-kafka:latest
```

## Configuration

All configuration is via environment variables.

### Required

| Variable                  | Description                                                          |
| ------------------------- | ------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers, e.g. `broker1:9092,broker2:9092`.          |
| `KAFKA_GROUP_ID`          | Kafka consumer group id (also the prefix of the dedup producer id). |
| `KAFKA_TOPICS`            | Comma-separated topics to subscribe to, e.g. `orders,payments`.     |
| `RESTATE_INGRESS_URL`     | Restate ingestion endpoint, `http://host:port` or `https://host`.   |
| `RESTATE_TARGET_SERVICE`  | Restate service to invoke for each record.                          |
| `RESTATE_TARGET_HANDLER`  | Handler on that service to invoke.                                  |

### Optional

| Variable                              | Default       | Description                                                     |
| ------------------------------------- | ------------- | --------------------------------------------------------------- |
| `RESTATE_KAFKA_CONSUMER_INSTANCES`    | 2 × CPU cores | Consumer instances per process (partition parallelism).         |
| `RESTATE_RETRY_INITIAL_INTERVAL_MS`   | `200`         | Initial reconnect backoff after a dropped ingestion stream.     |
| `RESTATE_RETRY_MAX_INTERVAL_MS`       | `30000`       | Maximum reconnect backoff.                                      |
| `RESTATE_RETRY_EXPONENTIATION_FACTOR` | `2.0`         | Backoff growth factor (≥ 1.0).                                 |
| `RESTATE_RETRY_MAX_ATTEMPTS`          | unbounded     | Give up (and exit) after this many consecutive failed attempts. |
| `CONFIG_FILE`                         | –             | Path to a `.properties` file of base Kafka config (env wins).   |

Logging is Log4j2; point it at your own config with `-Dlog4j2.configurationFile=/path/log4j2.xml`.

### Extra Kafka consumer settings

Any other `KAFKA_*` variable is forwarded to the Kafka consumer using the Confluent naming
convention — lowercased, with underscore runs mapped to separators: single `_` → `.`, double
`__` → `_`, triple `___` → `-`. For example `KAFKA_AUTO_OFFSET_RESET` → `auto.offset.reset`.

A SASL/TLS setup:

```bash
-e KAFKA_SECURITY_PROTOCOL=SASL_SSL \
-e KAFKA_SASL_MECHANISM=PLAIN \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";'
```

The deserializers and commit mode are fixed and cannot be overridden: keys are read as UTF-8
strings, values as raw bytes, and offsets are committed manually after Restate confirms them.

## Record mapping

| Kafka                                        | Restate `Record`                                                                          |
| -------------------------------------------- | ----------------------------------------------------------------------------------------- |
| key (UTF-8)                                  | `key` — the Virtual Object / Workflow key (required by Restate for VO/Workflow targets)   |
| value (bytes)                                | `payload` (a null value / tombstone → empty payload)                                      |
| topic / partition / offset / timestamp / key | request headers `kafka.topic`, `kafka.partition`, `kafka.offset`, `kafka.timestamp`, `kafka.key` |
| `traceparent` / `tracestate` headers         | propagated to `traceparent` / `tracestate`                                                |

## Building the image

Built with [Jib](https://github.com/GoogleContainerTools/jib) — no Dockerfile or Docker daemon
required:

```bash
./gradlew jibDockerBuild   # build into the local Docker/Podman daemon
./gradlew jibBuildTar      # build to build/jib-image.tar
./gradlew jib              # build and push to the configured registry
```
