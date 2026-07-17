package dev.restate.integration.kafka

import dev.restate.ingestion.v1.Record

/**
 * Maps a source record's fields into an ingestion [Record]. Operates on primitives (not
 * Vert.x/Kafka types) so it is trivially unit-testable; the verticle extracts the fields from
 * `KafkaConsumerRecord` and calls [toRecord]. Swap in a custom implementation to change the
 * mapping.
 */
interface RecordMapper {
  fun toRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Long,
      key: String?,
      value: ByteArray?,
      headers: List<Pair<String, ByteArray>> = emptyList(),
  ): Record
}
