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
    // Static fields are Settings defaults, not repeated per record.
    val settings = m.initialDefaults()
    assertThat(settings.service).isEqualTo("OrderService")
    assertThat(settings.scope).isEqualTo("orders")
    assertThat(settings.hasHandler()).isFalse() // handler is dynamic

    val invocation =
        m.toInvocation(
            record(key = "cust-1", value = """{"type":"create","id":"evt-9","amount":5}""")
        )

    // Dynamic fields are per-record overrides.
    assertThat(invocation.handler).isEqualTo("create")
    assertThat(invocation.key).isEqualTo("cust-1")
    assertThat(invocation.idempotencyKey).isEqualTo("evt-9")
    // Static fields live in Settings, not on the record.
    assertThat(invocation.hasService()).isFalse()
    assertThat(invocation.hasScope()).isFalse()
    // payload is the (re-serialized) JSON, semantically equal to the input
    assertThat(json.readTree(invocation.payload.toByteArray()))
        .isEqualTo(json.readTree("""{"type":"create","id":"evt-9","amount":5}"""))
  }

  @Test
  fun `dynamic fields land on the record, not settings`() {
    val m = mapper("service.pointer" to "/svc", "handler.value" to "greet")
    assertThat(m.initialDefaults().hasService()).isFalse()
    assertThat(m.initialDefaults().handler).isEqualTo("greet")

    val invocation = m.toInvocation(record(value = """{"svc":"Dyn"}"""))
    assertThat(invocation.service).isEqualTo("Dyn")
    assertThat(invocation.hasHandler()).isFalse() // static -> in settings
  }

  @Test
  fun `omits optional fields when not configured`() {
    val m = mapper("service.value" to "S", "handler.value" to "h")
    val settings = m.initialDefaults()
    assertThat(settings.service).isEqualTo("S")
    assertThat(settings.handler).isEqualTo("h")

    val invocation = m.toInvocation(record(key = "k", value = """{"x":1}"""))
    // static service/handler are Settings defaults, not per-record; optionals stay unset
    assertThat(invocation.hasService()).isFalse()
    assertThat(invocation.hasHandler()).isFalse()
    assertThat(invocation.hasKey()).isFalse()
    assertThat(invocation.hasIdempotencyKey()).isFalse()
    assertThat(invocation.hasScope()).isFalse()
    assertThat(invocation.hasLimitKey()).isFalse()
  }

  @Test
  fun `attaches kafka metadata headers by default`() {
    val m = mapper("service.value" to "S", "handler.value" to "h")
    val invocation = m.toInvocation(record(value = """{"x":1}""", offset = 42))
    assertThat(invocation.additionalHeadersMap)
        .containsEntry("kafka.topic", "t")
        .containsEntry("kafka.partition", "0")
        .containsEntry("kafka.offset", "42")
        .containsKey("kafka.timestamp")
  }

  @Test
  fun `kafka metadata false omits the kafka metadata headers`() {
    val m = mapper("service.value" to "S", "handler.value" to "h", "kafka.metadata" to "false")
    val invocation = m.toInvocation(record(value = """{"x":1}""", offset = 42))
    assertThat(invocation.additionalHeadersMap)
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
