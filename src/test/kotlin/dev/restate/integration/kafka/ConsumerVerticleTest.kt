// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.vertx.core.Vertx
import java.lang.management.ManagementFactory
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.function.ToDoubleFunction
import javax.management.ObjectName
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

/**
 * A failed [ConsumerVerticle.start] must not leak the Kafka consumer (Vert.x doesn't call stop()
 * for it). No broker is needed: the consumer never connects. A live Kafka consumer is observed via
 * the `kafka.consumer:type=app-info,id=<client.id>` MBean it registers until closed.
 */
class ConsumerVerticleTest {

  private val vertx = Vertx.vertx()

  @AfterEach
  fun tearDown() {
    vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
  }

  private fun config(clientId: String, vararg extra: Pair<String, String>) =
      RestateKafkaIntegrationConfiguration.load(
          mapOf(
              "KAFKA_BOOTSTRAP_SERVERS" to "localhost:1",
              "KAFKA_GROUP_ID" to "test",
              "KAFKA_CLIENT_ID" to clientId,
              "KAFKA_TOPICS" to "orders",
              "RESTATE_INGRESS_URL" to "http://localhost:1",
              "RESTATE_RECORD_MAPPER_SERVICE" to "Greeter",
              "RESTATE_RECORD_MAPPER_HANDLER" to "greet",
          ) + extra
      )

  private fun deploy(verticle: ConsumerVerticle) =
      vertx
          .deployVerticle(verticle)
          .toCompletionStage()
          .toCompletableFuture()
          .get(30, TimeUnit.SECONDS)

  private fun consumerOpen(clientId: String) =
      ManagementFactory.getPlatformMBeanServer()
          .isRegistered(ObjectName("kafka.consumer:type=app-info,id=$clientId"))

  @Test
  fun `closes the consumer when start fails before subscribing`() {
    // Binding the consumer metrics fails, i.e. after the consumer exists but before subscribe().
    val failingRegistry =
        object : SimpleMeterRegistry() {
          override fun <T : Any> newGauge(
              id: Meter.Id,
              obj: T?,
              valueFunction: ToDoubleFunction<T>,
          ): Gauge = throw IllegalStateException("boom")
        }

    assertThatThrownBy { deploy(ConsumerVerticle(config("pre-subscribe"), failingRegistry)) }
        .isInstanceOf(ExecutionException::class.java)
        .hasRootCauseMessage("boom")
    assertThat(consumerOpen("pre-subscribe")).isFalse()
  }

  @Test
  fun `closes the consumer when subscribe fails`() {
    // Without an assignor, subscribe() throws.
    val config = config("subscribe", "KAFKA_PARTITION_ASSIGNMENT_STRATEGY" to "")

    assertThatThrownBy { deploy(ConsumerVerticle(config)) }
        .isInstanceOf(ExecutionException::class.java)
        .hasRootCauseInstanceOf(IllegalStateException::class.java)
    assertThat(consumerOpen("subscribe")).isFalse()
  }
}
