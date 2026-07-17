package dev.restate.integration.kafka

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RecordMapperTest {

  @Test
  fun `maps key, value and kafka metadata headers`() {
    val record =
        DefaultRecordMapper.toRecord(
            topic = "orders",
            partition = 3,
            offset = 42,
            timestamp = 1234,
            key = "customer-7",
            value = "hello".toByteArray(),
        )

    assertThat(record.offset).isEqualTo(42L)
    assertThat(record.key).isEqualTo("customer-7")
    assertThat(record.payload.toStringUtf8()).isEqualTo("hello")
    assertThat(record.additionalHeadersMap)
        .containsEntry("kafka.topic", "orders")
        .containsEntry("kafka.partition", "3")
        .containsEntry("kafka.offset", "42")
        .containsEntry("kafka.timestamp", "1234")
        .containsEntry("kafka.key", "customer-7")
  }

  @Test
  fun `null key omits the key and its header`() {
    val record = DefaultRecordMapper.toRecord("t", 0, 0, 0, key = null, value = "v".toByteArray())
    assertThat(record.hasKey()).isFalse()
    assertThat(record.additionalHeadersMap).doesNotContainKey("kafka.key")
  }

  @Test
  fun `null value (tombstone) becomes an empty payload`() {
    val record = DefaultRecordMapper.toRecord("t", 0, 5, 0, key = "k", value = null)
    assertThat(record.payload.isEmpty).isTrue()
    assertThat(record.offset).isEqualTo(5L)
  }

  @Test
  fun `propagates w3c trace context from headers`() {
    val record =
        DefaultRecordMapper.toRecord(
            "t",
            0,
            0,
            0,
            key = "k",
            value = ByteArray(0),
            headers =
                listOf(
                    "traceparent" to "00-abc-def-01".toByteArray(),
                    "tracestate" to "rojo=00f".toByteArray(),
                    "other" to "ignored".toByteArray(),
                ),
        )
    assertThat(record.traceparent).isEqualTo("00-abc-def-01")
    assertThat(record.tracestate).isEqualTo("rojo=00f")
  }
}
