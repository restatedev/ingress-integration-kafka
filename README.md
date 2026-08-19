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
  of the box (see [Metrics](CONFIGURATION.md#observability)).

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

For a detailed description of all configuration options, see [Configuration](CONFIGURATION.md).

## Building the image

Built with [Jib](https://github.com/GoogleContainerTools/jib) — no Dockerfile or Docker daemon
required:

```bash
./gradlew jibDockerBuild   # build into the local Docker/Podman daemon
./gradlew jibBuildTar      # build to build/jib-image.tar
./gradlew jib              # build and push to the configured registry
```
