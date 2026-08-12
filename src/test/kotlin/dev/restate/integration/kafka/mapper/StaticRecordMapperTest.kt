package dev.restate.integration.kafka

import dev.restate.integration.kafka.mapper.StaticRecordMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class StaticRecordMapperTest {

  private val mapper =
      StaticRecordMapper().apply {
        configure(mutableMapOf("service" to "Greeter", "handler" to "greet"))
      }

  private fun record(
      topic: String = "t",
      partition: Int = 0,
      offset: Long = 0,
      key: String? = null,
      value: ByteArray? = null,
      headers: List<Pair<String, ByteArray>> = emptyList(),
  ): ConsumerRecord<String, ByteArray> {
    val record = ConsumerRecord<String, ByteArray>(topic, partition, offset, key, value)
    headers.forEach { (k, v) -> record.headers().add(k, v) }
    return record
  }

  @Test
  fun `initialSettings carries the configured target service and handler`() {
    val settings = mapper.initialDefaults()
    assertThat(settings.service).isEqualTo("Greeter")
    assertThat(settings.handler).isEqualTo("greet")
  }

  @Test
  fun `configure requires service and handler`() {
    assertThatThrownBy { StaticRecordMapper().configure(mutableMapOf("handler" to "greet")) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.service")
    assertThatThrownBy { StaticRecordMapper().configure(mutableMapOf("service" to "Greeter")) }
        .isInstanceOf(IllegalArgumentException::class.java)
        .hasMessageContaining("restate.record.mapper.handler")
  }

  @Test
  fun `maps key, value and kafka metadata headers`() {
    val record =
        mapper.toInvocation(
            record(
                topic = "orders",
                partition = 3,
                offset = 42,
                key = "customer-7",
                value = "hello".toByteArray(),
            )
        )

    assertThat(record.offset).isEqualTo(42L)
    assertThat(record.key).isEqualTo("customer-7")
    assertThat(record.payload.toStringUtf8()).isEqualTo("hello")
    assertThat(record.additionalHeadersMap)
        .containsEntry("kafka.topic", "orders")
        .containsEntry("kafka.partition", "3")
        .containsEntry("kafka.offset", "42")
        .containsEntry("kafka.key", "customer-7")
        .containsKey("kafka.timestamp")
  }

  @Test
  fun `kafka metadata false omits the kafka metadata headers`() {
    val m =
        StaticRecordMapper().apply {
          configure(
              mutableMapOf(
                  "service" to "Greeter",
                  "handler" to "greet",
                  "kafka.metadata" to "false",
              )
          )
        }
    val record =
        m.toInvocation(record(topic = "orders", partition = 3, offset = 42, key = "customer-7"))

    assertThat(record.additionalHeadersMap)
        .doesNotContainKey("kafka.topic")
        .doesNotContainKey("kafka.partition")
        .doesNotContainKey("kafka.offset")
        .doesNotContainKey("kafka.timestamp")
    // key handling is independent of the metadata flag
    assertThat(record.key).isEqualTo("customer-7")
    assertThat(record.additionalHeadersMap).containsEntry("kafka.key", "customer-7")
  }

  @Test
  fun `null key omits the key and its header`() {
    val record = mapper.toInvocation(record(key = null, value = "v".toByteArray()))
    assertThat(record.hasKey()).isFalse()
    assertThat(record.additionalHeadersMap).doesNotContainKey("kafka.key")
  }

  @Test
  fun `null value (tombstone) becomes an empty payload`() {
    val record = mapper.toInvocation(record(offset = 5, key = "k", value = null))
    assertThat(record.payload.isEmpty).isTrue()
    assertThat(record.offset).isEqualTo(5L)
  }

  @Test
  fun `propagates w3c trace context from headers`() {
    val record =
        mapper.toInvocation(
            record(
                key = "k",
                value = ByteArray(0),
                headers =
                    listOf(
                        "traceparent" to "00-abc-def-01".toByteArray(),
                        "tracestate" to "rojo=00f".toByteArray(),
                        "other" to "ignored".toByteArray(),
                    ),
            )
        )
    assertThat(record.traceparent).isEqualTo("00-abc-def-01")
    assertThat(record.tracestate).isEqualTo("rojo=00f")
  }
}
