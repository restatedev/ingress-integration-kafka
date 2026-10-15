// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import dev.restate.integration.kafka.mapper.JsonDynamicTargetRecordMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class JsonDynamicTargetRecordMapperTest {

  private val json = ObjectMapper()

  private fun mapper(vararg config: Pair<String, String>) =
      JsonDynamicTargetRecordMapper().apply { configure(config.toMap().toMutableMap()) }

  private fun record(
      key: String? = null,
      value: String? = null,
      offset: Long = 0,
      headers: List<Pair<String, ByteArray>> = emptyList(),
  ): ConsumerRecord<String, JsonNode> {
    val node: JsonNode? = value?.let { json.readTree(it) }
    val record = ConsumerRecord<String, JsonNode>("t", 0, offset, key, node)
    headers.forEach { (k, v) -> record.headers().add(k, v) }
    return record
  }

  @Test
  fun `extracts static, key and json-pointer sources`() {
    val m =
        mapper(
            "service.value" to "OrderService",
            "handler.pointer" to "/type",
            "key.fromkey" to "true",
            "idempotencykey.pointer" to "/id",
            "scope.value" to "orders",
        )
    // Static fields are stream defaults, not repeated per record.
    val defaults = m.initialDefaults()
    assertThat(defaults.serviceName).isEqualTo("OrderService")
    assertThat(defaults.scope).isEqualTo("orders")
    assertThat(defaults.handlerName).isNull() // handler is dynamic

    val invocation =
        m.toInvocation(
            record(key = "cust-1", value = """{"type":"create","id":"evt-9","amount":5}""")
        )

    // Dynamic fields are per-record overrides.
    assertThat(invocation.handlerName).isEqualTo("create")
    assertThat(invocation.key).isEqualTo("cust-1")
    assertThat(invocation.idempotencyKey).isEqualTo("evt-9")
    // Static fields live in the defaults, not on the record.
    assertThat(invocation.serviceName).isNull()
    assertThat(invocation.scope).isNull()
    // payload is the (re-serialized) JSON, semantically equal to the input
    assertThat(json.readTree(invocation.body))
        .isEqualTo(json.readTree("""{"type":"create","id":"evt-9","amount":5}"""))
  }

  @Test
  fun `dynamic fields land on the record, not defaults`() {
    val m = mapper("service.pointer" to "/svc", "handler.value" to "greet")
    assertThat(m.initialDefaults().serviceName).isNull()
    assertThat(m.initialDefaults().handlerName).isEqualTo("greet")

    val invocation = m.toInvocation(record(value = """{"svc":"Dyn"}"""))
    assertThat(invocation.serviceName).isEqualTo("Dyn")
    assertThat(invocation.handlerName).isNull() // static -> in defaults
  }

  @Test
  fun `omits optional fields when not configured`() {
    val m = mapper("service.value" to "S", "handler.value" to "h")
    val defaults = m.initialDefaults()
    assertThat(defaults.serviceName).isEqualTo("S")
    assertThat(defaults.handlerName).isEqualTo("h")

    val invocation = m.toInvocation(record(key = "k", value = """{"x":1}"""))
    // static service/handler are defaults, not per-record; optionals stay unset
    assertThat(invocation.serviceName).isNull()
    assertThat(invocation.handlerName).isNull()
    assertThat(invocation.key).isNull()
    assertThat(invocation.idempotencyKey).isNull()
    assertThat(invocation.scope).isNull()
    assertThat(invocation.limitKey).isNull()
  }

  @Test
  fun `attaches kafka metadata headers by default`() {
    val m = mapper("service.value" to "S", "handler.value" to "h")
    val invocation = m.toInvocation(record(value = """{"x":1}""", offset = 42))
    assertThat(invocation.headers)
        .containsEntry("kafka.topic", "t")
        .containsEntry("kafka.partition", "0")
        .containsEntry("kafka.offset", "42")
        .containsKey("kafka.timestamp")
  }

  @Test
  fun `kafka metadata false omits the kafka metadata headers`() {
    val m = mapper("service.value" to "S", "handler.value" to "h", "kafka.metadata" to "false")
    val invocation = m.toInvocation(record(value = """{"x":1}""", offset = 42))
    assertThat(invocation.headers)
        .doesNotContainKey("kafka.topic")
        .doesNotContainKey("kafka.partition")
        .doesNotContainKey("kafka.offset")
        .doesNotContainKey("kafka.timestamp")
  }

  @Test
  fun `service is required`() {
    assertThatThrownBy { mapper("handler.value" to "h") }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.service")
  }

  @Test
  fun `rejects more than one source for a field`() {
    assertThatThrownBy {
          mapper("service.value" to "S", "service.pointer" to "/x", "handler.value" to "h")
        }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.service")
  }

  @Test
  fun `propagates w3c trace context from headers`() {
    val m = mapper("service.value" to "S", "handler.value" to "h")
    val invocation =
        m.toInvocation(
            record(
                value = """{"x":1}""",
                headers =
                    listOf(
                        "traceparent" to "00-abc-01".toByteArray(),
                        "tracestate" to "s=1".toByteArray(),
                    ),
            )
        )
    assertThat(invocation.traceparent).isEqualTo("00-abc-01")
    assertThat(invocation.tracestate).isEqualTo("s=1")
  }
}
