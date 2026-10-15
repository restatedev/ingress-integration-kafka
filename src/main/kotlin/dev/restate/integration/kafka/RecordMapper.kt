// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka

import dev.restate.integration.Invocation
import dev.restate.integration.InvocationMetadata
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

/**
 * Maps a Kafka source record to a Restate ingestion [Invocation], and owns how Kafka bytes become
 * that invocation: it declares the [Deserializer]s for its key/value types, so the consumer's
 * deserialization (and thus [K]/[V]) is driven by the mapper. Swap in a custom implementation via
 * `restate.record.mapper.class`; configure it via `restate.record.mapper.*`.
 *
 * The offset is **not** the mapper's concern: the runtime assigns each invocation the Kafka record
 * offset (it drives exactly-once dedup), so [toInvocation] only fills in the target, key, payload,
 * headers, etc.
 */
@Suppress("UnstableApiUsage")
interface RecordMapper<K, V> {

  /** Deserializer that turns the Kafka record key into a [K]. */
  val keyDeserializer: Class<out Deserializer<K>>

  /** Deserializer that turns the Kafka record value into a [V]. */
  val valueDeserializer: Class<out Deserializer<V>>

  /** The stream defaults (target service/handler, headers, ...) applied to every record. */
  fun initialDefaults(): InvocationMetadata

  /** Map one consumed record to an ingestion invocation, or return `null` to filter it out. */
  fun toInvocation(record: ConsumerRecord<K, V>): Invocation?
}
