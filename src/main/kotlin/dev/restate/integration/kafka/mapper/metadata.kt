package dev.restate.integration.kafka.mapper

import dev.restate.ingestion.v1.Invocation
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Shared handling of the `kafka.*` record-metadata headers (`kafka.topic`, `kafka.partition`,
 * `kafka.offset`, `kafka.timestamp`). Both [StaticRecordMapper] and [JsonDynamicTargetRecordMapper]
 * attach them and gate them behind the same config flag.
 */

/** Config sub-key (`restate.record.mapper.kafka.metadata`) toggling the metadata headers. */
internal const val KAFKA_METADATA = "kafka.metadata"

/** Whether the [KAFKA_METADATA] flag is enabled; absent (or blank) means enabled. */
internal fun Map<String, *>.kafkaMetadataEnabled(): Boolean =
    (this[KAFKA_METADATA] as? String)?.takeIf { it.isNotBlank() }?.toBoolean() ?: true

/** Attach the `kafka.topic`/`kafka.partition`/`kafka.offset`/`kafka.timestamp` headers. */
internal fun Invocation.Builder.putKafkaMetadataHeaders(
    record: ConsumerRecord<*, *>
): Invocation.Builder =
    putAdditionalHeaders("kafka.topic", record.topic())
        .putAdditionalHeaders("kafka.partition", record.partition().toString())
        .putAdditionalHeaders("kafka.offset", record.offset().toString())
        .putAdditionalHeaders("kafka.timestamp", record.timestamp().toString())
