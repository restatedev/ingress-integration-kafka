// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka.mapper

import dev.restate.integration.Invocation
import dev.restate.integration.InvocationMetadata
import dev.restate.integration.kafka.RecordMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * The default [dev.restate.integration.kafka.RecordMapper], used when the user hasn't set
 * `restate.record.mapper.class`: static parity with the built-in Kafka consumer. Every record
 * targets the same service/handler; the Kafka key becomes the VO/Workflow key, the value the
 * payload, and (unless `kafka.metadata` is disabled) `kafka.*` metadata plus W3C trace context are
 * attached as additional headers. No content-type is set.
 */
class StaticRecordMapper : RecordMapper<String, ByteArray>, Configurable {

  override val keyDeserializer: Class<out Deserializer<String>> = StringDeserializer::class.java
  override val valueDeserializer: Class<out Deserializer<ByteArray>> =
      ByteArrayDeserializer::class.java

  private lateinit var targetService: String
  private lateinit var targetHandler: String
  private var kafkaMetadata: Boolean = true

  override fun configure(configs: MutableMap<String, *>) {
    targetService =
        (configs[SERVICE] as? String)?.takeIf { it.isNotBlank() }
            ?: throw IllegalArgumentException(
                "restate.record.mapper.$SERVICE is required for ${javaClass.name}"
            )
    targetHandler =
        (configs[HANDLER] as? String)?.takeIf { it.isNotBlank() }
            ?: throw IllegalArgumentException(
                "restate.record.mapper.$HANDLER is required for ${javaClass.name}"
            )
    kafkaMetadata = configs.kafkaMetadataEnabled()
  }

  override fun initialDefaults(): InvocationMetadata =
      InvocationMetadata.create().setServiceName(targetService).setHandlerName(targetHandler)

  override fun toInvocation(record: ConsumerRecord<String, ByteArray>): Invocation {
    // A null Kafka value (tombstone) clears the body, which the client sends as an empty payload.
    val invocation = Invocation.create().setBody(record.value())

    if (kafkaMetadata) {
      invocation.putKafkaMetadataHeaders(record)
    }

    record.key()?.let {
      // The VO/Workflow key + a convenience header. A null key means no key (the server rejects it
      // if the target is a virtual object).
      invocation.setKey(it)
      invocation.putHeader("kafka.key", it)
    }

    for (header in record.headers()) {
      val value = header.value() ?: continue
      when (header.key()) {
        TRACEPARENT -> invocation.setTraceparent(String(value, Charsets.UTF_8))
        TRACESTATE -> invocation.setTracestate(String(value, Charsets.UTF_8))
      }
    }

    return invocation
  }

  companion object {
    const val SERVICE = "service"
    const val HANDLER = "handler"
    private const val TRACEPARENT = "traceparent"
    private const val TRACESTATE = "tracestate"
  }
}
