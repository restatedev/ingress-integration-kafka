package dev.restate.integration.kafka

import com.google.protobuf.ByteString
import dev.restate.ingestion.v1.Record

/**
 * Maps a Kafka record into an ingestion [Record]. Pure (operates on primitives, not Vert.x/Kafka
 * types) so it is trivially unit-testable; the verticle extracts the fields from
 * `KafkaConsumerRecord` and calls [toRecord].
 *
 * Parity with the built-in consumer: the Kafka key becomes the VO/Workflow key, the value becomes
 * the payload, and `kafka.*` metadata is attached as headers. W3C trace context is propagated from
 * the record headers when present. No content-type is set.
 */
object RecordMapper {

  private const val TRACEPARENT = "traceparent"
  private const val TRACESTATE = "tracestate"

  fun toRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Long,
      key: String?,
      value: ByteArray?,
      headers: List<Pair<String, ByteArray>> = emptyList(),
  ): Record {
    val builder =
        Record.newBuilder()
            .setOffset(offset)
            .setPayload(if (value != null) ByteString.copyFrom(value) else ByteString.EMPTY)
            .putAdditionalHeaders("kafka.topic", topic)
            .putAdditionalHeaders("kafka.partition", partition.toString())
            .putAdditionalHeaders("kafka.offset", offset.toString())
            .putAdditionalHeaders("kafka.timestamp", timestamp.toString())

    if (key != null) {
      // The VO/Workflow key + a convenience header. A null key means no key (server rejects it if
      // the target is a virtual object).
      builder.key = key
      builder.putAdditionalHeaders("kafka.key", key)
    }

    for ((name, raw) in headers) {
      when (name) {
        TRACEPARENT -> builder.traceparent = String(raw, Charsets.UTF_8)
        TRACESTATE -> builder.tracestate = String(raw, Charsets.UTF_8)
      }
    }

    return builder.build()
  }
}
