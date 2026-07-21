package dev.restate.integration.kafka

import dev.restate.integration.client.IngressEndpoint
import io.vertx.core.VertxOptions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class AppConfigTest {

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

    val cfg = AppConfig.load(env)

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

    val cfg = AppConfig.load(env)

    assertThat(cfg.groupId).isEqualTo("my-group")
    assertThat(cfg.restate.topics).containsExactly("orders")
    assertThat(cfg.recordMapper.initialSettings().service).isEqualTo("Greeter")
    assertThat(cfg.recordMapper.initialSettings().handler).isEqualTo("greet")
    assertThat(cfg.restate.ingress).isEqualTo(IngressEndpoint("localhost", 8080, false))
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

    val cfg = AppConfig.load(env)

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
    val cfg = AppConfig.load(baseEnv())
    assertThat(cfg.kafkaConsumerConfig).doesNotContainKey("topics")
  }

  @Test
  fun `parses multiple topics and trims whitespace`() {
    val env = baseEnv()
    env["KAFKA_TOPICS"] = " orders , payments , shipments "
    val cfg = AppConfig.load(env)
    assertThat(cfg.restate.topics).containsExactly("orders", "payments", "shipments")
  }

  @Test
  fun `env overrides properties file`() {
    val file = mapOf("bootstrap.servers" to "file-broker:9092", "group.id" to "file-group")
    val env = baseEnv() // KAFKA_BOOTSTRAP_SERVERS=broker:9092, KAFKA_GROUP_ID=my-group
    val cfg = AppConfig.load(env, fileConfig = file)
    assertThat(cfg.kafkaConsumerConfig).containsEntry("bootstrap.servers", "broker:9092")
    assertThat(cfg.groupId).isEqualTo("my-group")
  }

  @Test
  fun `parses https ingress url as TLS with default port`() {
    val env = baseEnv()
    env["RESTATE_INGRESS_URL"] = "https://ingress.example.com"
    val cfg = AppConfig.load(env)
    assertThat(cfg.restate.ingress).isEqualTo(IngressEndpoint("ingress.example.com", 443, true))
  }

  @Test
  fun `honours RESTATE_KAFKA_CONSUMER_INSTANCES override`() {
    val env = baseEnv()
    env["RESTATE_KAFKA_CONSUMER_INSTANCES"] = "3"
    assertThat(AppConfig.load(env).restate.consumerInstances).isEqualTo(3)
  }

  @Test
  fun `metrics are enabled by default on port 9464`() {
    val cfg = AppConfig.load(baseEnv())
    assertThat(cfg.restate.metricsEnabled).isTrue()
    assertThat(cfg.restate.metricsPort).isEqualTo(9464)
  }

  @Test
  fun `honours RESTATE_METRICS_ENABLED and RESTATE_METRICS_PORT overrides`() {
    val env = baseEnv()
    env["RESTATE_METRICS_ENABLED"] = "false"
    env["RESTATE_METRICS_PORT"] = "8081"
    val cfg = AppConfig.load(env)
    assertThat(cfg.restate.metricsEnabled).isFalse()
    assertThat(cfg.restate.metricsPort).isEqualTo(8081)
  }

  @Test
  fun `rejects an out-of-range metrics port`() {
    assertThatThrownBy {
          AppConfig.load(baseEnv().apply { this["RESTATE_METRICS_PORT"] = "70000" })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
  }

  @Test
  fun `rejects missing bootstrap servers`() {
    val env = baseEnv().apply { remove("KAFKA_BOOTSTRAP_SERVERS") }
    assertThatThrownBy { AppConfig.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("bootstrap.servers")
  }

  @Test
  fun `rejects missing group id`() {
    val env = baseEnv().apply { remove("KAFKA_GROUP_ID") }
    assertThatThrownBy { AppConfig.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("group.id")
  }

  @Test
  fun `rejects missing topics`() {
    val env = baseEnv().apply { remove("KAFKA_TOPICS") }
    assertThatThrownBy { AppConfig.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("topics")
  }

  @Test
  fun `rejects missing target service and handler`() {
    assertThatThrownBy {
          AppConfig.load(baseEnv().apply { remove("RESTATE_RECORD_MAPPER_SERVICE") })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.service")
    assertThatThrownBy {
          AppConfig.load(baseEnv().apply { remove("RESTATE_RECORD_MAPPER_HANDLER") })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.handler")
  }

  @Test
  fun `rejects invalid ingress url scheme`() {
    val env = baseEnv().apply { this["RESTATE_INGRESS_URL"] = "tcp://foo:1234" }
    assertThatThrownBy { AppConfig.load(env) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("http or https")
  }

  @Test
  fun `rejects non-integer and non-positive instance counts`() {
    assertThatThrownBy {
          AppConfig.load(baseEnv().apply { this["RESTATE_KAFKA_CONSUMER_INSTANCES"] = "abc" })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
    assertThatThrownBy {
          AppConfig.load(baseEnv().apply { this["RESTATE_KAFKA_CONSUMER_INSTANCES"] = "0" })
        }
        .isInstanceOf(IllegalArgumentException::class.java)
  }
}
