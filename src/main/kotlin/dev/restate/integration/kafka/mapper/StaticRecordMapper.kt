package dev.restate.integration.kafka.mapper

import com.google.protobuf.ByteString
import dev.restate.ingestion.v1.IngestionInvocation
import dev.restate.integration.client.StreamDefaults
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

  override fun initialDefaults(): StreamDefaults =
      StreamDefaults.newBuilder().setService(targetService).setHandler(targetHandler).build()

  override fun toInvocation(record: ConsumerRecord<String, ByteArray>): IngestionInvocation {
    val builder =
        IngestionInvocation.newBuilder()
            .setOffset(record.offset())
            .setPayload(record.value()?.let { ByteString.copyFrom(it) } ?: ByteString.EMPTY)

    if (kafkaMetadata) {
      builder.putKafkaMetadataHeaders(record)
    }

    record.key()?.let {
      // The VO/Workflow key + a convenience header. A null key means no key (the server rejects it
      // if the target is a virtual object).
      builder.key = it
      builder.putAdditionalHeaders("kafka.key", it)
    }

    for (header in record.headers()) {
      val value = header.value() ?: continue
      when (header.key()) {
        TRACEPARENT -> builder.traceparent = String(value, Charsets.UTF_8)
        TRACESTATE -> builder.tracestate = String(value, Charsets.UTF_8)
      }
    }

    return builder.build()
  }

  companion object {
    const val SERVICE = "service"
    const val HANDLER = "handler"
    private const val TRACEPARENT = "traceparent"
    private const val TRACESTATE = "tracestate"
  }
}
