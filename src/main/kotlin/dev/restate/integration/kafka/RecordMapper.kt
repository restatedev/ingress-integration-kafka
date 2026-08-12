package dev.restate.integration.kafka

import dev.restate.ingestion.v1.IngestionInvocation
import dev.restate.integration.client.StreamDefaults
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

/**
 * Maps a Kafka source record to a Restate ingestion [IngestionInvocation], and owns how Kafka bytes
 * become that invocation: it declares the [Deserializer]s for its key/value types, so the
 * consumer's deserialization (and thus [K]/[V]) is driven by the mapper. Swap in a custom
 * implementation via `restate.record.mapper.class`; configure it via `restate.record.mapper.*`.
 */
interface RecordMapper<K, V> {

  /** Deserializer that turns the Kafka record key into a [K]. */
  val keyDeserializer: Class<out Deserializer<K>>

  /** Deserializer that turns the Kafka record value into a [V]. */
  val valueDeserializer: Class<out Deserializer<V>>

  /** The stream settings (target service/handler, headers, ...) for a new producer stream. */
  fun initialDefaults(): StreamDefaults

  /** Map one consumed record to an ingestion invocation, or return `null` to filter it out. */
  fun toInvocation(record: ConsumerRecord<K, V>): IngestionInvocation?
}
