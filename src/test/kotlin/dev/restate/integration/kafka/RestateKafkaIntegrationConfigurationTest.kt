// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka

import dev.restate.integration.kafka.ingestion.IngressEndpoint
import io.vertx.core.VertxOptions
import java.io.File
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class RestateKafkaIntegrationConfigurationTest {

  private fun baseEnv() =
      mutableMapOf(
          "KAFKA_BOOTSTRAP_SERVERS" to "broker:9092",
          "KAFKA_GROUP_ID" to "my-group",
          "KAFKA_TOPICS" to "orders",
          "RESTATE_INGRESS_URL" to "http://localhost:8080",
          "RESTATE_RECORD_MAPPER_SERVICE" to "Greeter",
          "RESTATE_RECORD_MAPPER_HANDLER" to "greet",
      )

  @Test
  fun `maps KAFKA_ env names to kafka properties via the Confluent convention`() {
    val env =
        baseEnv().apply {
          this["KAFKA_SASL_JAAS_CONFIG"] = "jaas-cfg"
          this["KAFKA_AUTO_OFFSET_RESET"] = "earliest"
          this["KAFKA_FOO__BAR"] = "double" // double underscore -> literal underscore
          this["KAFKA_FOO___BAR"] = "triple" // triple underscore -> dash
        }

    val cfg = RestateKafkaIntegrationConfiguration.load(env)

    assertThat(cfg.kafkaConsumerConfig)
        .containsEntry("bootstrap.servers", "broker:9092")
        .containsEntry("group.id", "my-group")
        .containsEntry("sasl.jaas.config", "jaas-cfg")
        .containsEntry("auto.offset.reset", "earliest")
        .containsEntry("foo_bar", "double")
        .containsEntry("foo-bar", "triple")
  }

  @Test
  fun `loads a valid configuration`() {
    val env = baseEnv()
    env["KAFKA_AUTO_OFFSET_RESET"] = "earliest"

    val cfg = RestateKafkaIntegrationConfiguration.load(env)

    assertThat(cfg.groupId).isEqualTo("my-group")
    assertThat(cfg.restate.topics).containsExactly("orders")
    assertThat(cfg.recordMapper.initialDefaults().serviceName).isEqualTo("Greeter")
    assertThat(cfg.recordMapper.initialDefaults().handlerName).isEqualTo("greet")
    assertThat(cfg.restate.ingress).containsOnly(IngressEndpoint("localhost", 8080, false))
    // Defaults to Vert.x's event-loop pool size (one consumer instance per event loop).
    assertThat(cfg.restate.consumerInstances).isEqualTo(VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE)
    assertThat(cfg.kafkaConsumerConfig)
        .containsEntry("bootstrap.servers", "broker:9092")
        .containsEntry("group.id", "my-group")
        .containsEntry("auto.offset.reset", "earliest")
  }

  @Test
  fun `forces deserializers and disables auto-commit even if the user sets them`() {
    val env = baseEnv()
    env["KAFKA_ENABLE_AUTO_COMMIT"] = "true"
    env["KAFKA_VALUE_DESERIALIZER"] = "org.apache.kafka.common.serialization.StringDeserializer"

    val cfg = RestateKafkaIntegrationConfiguration.load(env)

    assertThat(cfg.kafkaConsumerConfig)
        .containsEntry("enable.auto.commit", "false")
        .containsEntry(
            "value.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        )
        .containsEntry(
            "key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer",
        )
  }

  @Test
  fun `does not forward KAFKA_TOPICS as a kafka property`() {
    val cfg = RestateKafkaIntegrationConfiguration.load(baseEnv())
    assertThat(cfg.kafkaConsumerConfig).doesNotContainKey("topics")
  }

  @Test
  fun `parses multiple topics and trims whitespace`() {
    val env = baseEnv()
    env["KAFKA_TOPICS"] = " orders , payments , shipments "
    val cfg = RestateKafkaIntegrationConfiguration.load(env)
    assertThat(cfg.restate.topics).containsExactly("orders", "payments", "shipments")
  }

  @Test
  fun `env overrides properties file`() {
    val file = mapOf("bootstrap.servers" to "file-broker:9092", "group.id" to "file-group")
    val env = baseEnv() // KAFKA_BOOTSTRAP_SERVERS=broker:9092, KAFKA_GROUP_ID=my-group
    val cfg = RestateKafkaIntegrationConfiguration.load(env, fileConfig = file)
    assertThat(cfg.kafkaConsumerConfig).containsEntry("bootstrap.servers", "broker:9092")
    assertThat(cfg.groupId).isEqualTo("my-group")
  }

  @Test
  fun `loadPropertiesFiles returns empty for null or blank`() {
    assertThat(RestateKafkaIntegrationConfiguration.loadPropertiesFiles(null)).isEmpty()
    assertThat(RestateKafkaIntegrationConfiguration.loadPropertiesFiles("   ")).isEmpty()
  }

  @Test
  fun `loadPropertiesFiles reads a single file`(@TempDir dir: Path) {
    val file = writeProps(dir, "a.properties", "bootstrap.servers=file-broker:9092", "group.id=g")
    assertThat(RestateKafkaIntegrationConfiguration.loadPropertiesFiles(file))
        .containsEntry("bootstrap.servers", "file-broker:9092")
        .containsEntry("group.id", "g")
  }

  @Test
  fun `loadPropertiesFiles merges multiple files with the last winning`(@TempDir dir: Path) {
    val first = writeProps(dir, "first.properties", "bootstrap.servers=first:9092", "group.id=g")
    val second = writeProps(dir, "second.properties", "bootstrap.servers=second:9092")

    val merged = RestateKafkaIntegrationConfiguration.loadPropertiesFiles("$first,$second")

    // Later file overrides the earlier one on collision, but non-overridden keys are kept.
    assertThat(merged)
        .containsEntry("bootstrap.servers", "second:9092")
        .containsEntry("group.id", "g")
  }

  @Test
  fun `loadPropertiesFiles trims whitespace and skips blank entries`(@TempDir dir: Path) {
    val first = writeProps(dir, "first.properties", "a=1")
    val second = writeProps(dir, "second.properties", "b=2")

    val merged = RestateKafkaIntegrationConfiguration.loadPropertiesFiles("  $first , , $second  ,")

    assertThat(merged).containsEntry("a", "1").containsEntry("b", "2")
  }

  @Test
  fun `loadPropertiesFiles fails when any listed file is missing`(@TempDir dir: Path) {
    val existing = writeProps(dir, "a.properties", "a=1")
    val missing = dir.resolve("nope.properties").toString()

    assertThatThrownBy { RestateKafkaIntegrationConfiguration.loadPropertiesFiles("$existing,$missing") }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("missing file")
        .hasMessageContaining(missing)
  }

  @Test
  fun `parses https ingress url as TLS with default port`() {
    val env = baseEnv()
    env["RESTATE_INGRESS_URL"] = "https://ingress.example.com"
    val cfg = RestateKafkaIntegrationConfiguration.load(env)
    assertThat(cfg.restate.ingress).containsOnly(IngressEndpoint("ingress.example.com", 443, true))
  }

  @Test
  fun `parses multiple ingress urls`() {
    val env = baseEnv()
    env["RESTATE_INGRESS_URL"] = "https://ingress1.example.com, https://ingress2.example.com"
    val cfg = RestateKafkaIntegrationConfiguration.load(env)
    assertThat(cfg.restate.ingress)
        .containsExactly(
            IngressEndpoint("ingress1.example.com", 443, true),
            IngressEndpoint("ingress2.example.com", 443, true),
        )
  }

  @Test
  fun `honours RESTATE_KAFKA_CONSUMER_INSTANCES override`() {
    val env = baseEnv()
    env["RESTATE_KAFKA_CONSUMER_INSTANCES"] = "3"
    assertThat(RestateKafkaIntegrationConfiguration.load(env).restate.consumerInstances).isEqualTo(3)
  }

  @Test
  fun `metrics are enabled by default on port 9464`() {
    val cfg = RestateKafkaIntegrationConfiguration.load(baseEnv())
    assertThat(cfg.restate.metricsEnabled).isTrue()
    assertThat(cfg.restate.metricsPort).isEqualTo(9464)
  }

  @Test
  fun `honours RESTATE_METRICS_ENABLED and RESTATE_METRICS_PORT overrides`() {
    val env = baseEnv()
    env["RESTATE_METRICS_ENABLED"] = "false"
    env["RESTATE_METRICS_PORT"] = "8081"
    val cfg = RestateKafkaIntegrationConfiguration.load(env)
    assertThat(cfg.restate.metricsEnabled).isFalse()
    assertThat(cfg.restate.metricsPort).isEqualTo(8081)
  }

  @Test
  fun `rejects an out-of-range metrics port`() {
    assertThatThrownBy {
          RestateKafkaIntegrationConfiguration.load(baseEnv().apply { this["RESTATE_METRICS_PORT"] = "70000" })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
  }

  @Test
  fun `rejects missing bootstrap servers`() {
    val env = baseEnv().apply { remove("KAFKA_BOOTSTRAP_SERVERS") }
    assertThatThrownBy { RestateKafkaIntegrationConfiguration.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("bootstrap.servers")
  }

  @Test
  fun `rejects missing group id`() {
    val env = baseEnv().apply { remove("KAFKA_GROUP_ID") }
    assertThatThrownBy { RestateKafkaIntegrationConfiguration.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("group.id")
  }

  @Test
  fun `rejects missing topics`() {
    val env = baseEnv().apply { remove("KAFKA_TOPICS") }
    assertThatThrownBy { RestateKafkaIntegrationConfiguration.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("topics")
  }

  @Test
  fun `rejects missing target service and handler`() {
    assertThatThrownBy {
          RestateKafkaIntegrationConfiguration.load(baseEnv().apply { remove("RESTATE_RECORD_MAPPER_SERVICE") })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.service")
    assertThatThrownBy {
          RestateKafkaIntegrationConfiguration.load(baseEnv().apply { remove("RESTATE_RECORD_MAPPER_HANDLER") })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.handler")
  }

  @Test
  fun `rejects invalid ingress url scheme`() {
    val env = baseEnv().apply { this["RESTATE_INGRESS_URL"] = "tcp://foo:1234" }
    assertThatThrownBy { RestateKafkaIntegrationConfiguration.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("http or https")
  }

  @Test
  fun `rejects non-integer and non-positive instance counts`() {
    assertThatThrownBy {
          RestateKafkaIntegrationConfiguration.load(baseEnv().apply { this["RESTATE_KAFKA_CONSUMER_INSTANCES"] = "abc" })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
    assertThatThrownBy {
          RestateKafkaIntegrationConfiguration.load(baseEnv().apply { this["RESTATE_KAFKA_CONSUMER_INSTANCES"] = "0" })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
  }

  private fun writeProps(dir: Path, name: String, vararg lines: String): String {
    val file = File(dir.toFile(), name)
    file.writeText(lines.joinToString("\n"))
    return file.absolutePath
  }
}
