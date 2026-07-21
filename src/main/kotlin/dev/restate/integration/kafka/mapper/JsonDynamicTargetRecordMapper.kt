package dev.restate.integration.kafka

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Settings
import dev.restate.integration.client.Invocation
import dev.restate.integration.client.StreamSettings
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * A [RecordMapper] that parses each Kafka record value as JSON and derives the invocation's target
 * (and optional key/idempotency-key/scope/limit-key) dynamically. Each field is configured
 * independently by setting **exactly one** of three sub-keys:
 * - `<field>.value = <literal>` — a static value;
 * - `<field>.fromkey = true` — the Kafka record key (string);
 * - `<field>.pointer = <jsonPointer>` — a value pulled from the JSON payload via a Jackson
 *   [JsonPointer], e.g. `<field>.pointer = /order/service`.
 *
 * Fields (under `restate.record.mapper.*`, i.e. `RESTATE_RECORD_MAPPER_*` env): `service`
 * (required), `handler` (required), `key`, `idempotency.key`, `scope`, `limit.key`. Example:
 * `RESTATE_RECORD_MAPPER_SERVICE_VALUE=OrderService`,
 * `RESTATE_RECORD_MAPPER_HANDLER_POINTER=/type`, `RESTATE_RECORD_MAPPER_KEY_FROMKEY=true`.
 *
 * Static fields become [Settings] defaults (set once in [initialSettings]); dynamically-derived
 * fields (from the key or a JSON pointer) are set per-[Record], overriding those defaults. The
 * payload is the (re-serialized) JSON; W3C trace context is propagated from the Kafka record
 * headers exactly like [StaticRecordMapper].
 */
class JsonDynamicTargetRecordMapper : RecordMapper<String, JsonNode>, Configurable {

  override val keyDeserializer: Class<out Deserializer<String>> = StringDeserializer::class.java
  override val valueDeserializer: Class<out Deserializer<JsonNode>> =
      JsonNodeDeserializer::class.java

  private lateinit var service: FieldSource
  private lateinit var handler: FieldSource
  private var key: FieldSource? = null
  private var idempotencyKey: FieldSource? = null
  private var scope: FieldSource? = null
  private var limitKey: FieldSource? = null

  override fun configure(configs: MutableMap<String, *>) {
    fun source(field: String, required: Boolean): FieldSource? {
      val fromKey = (configs["$field.$FROMKEY"] as? String)?.toBoolean() ?: false
      val pointer = (configs["$field.$POINTER"] as? String)?.takeIf { it.isNotBlank() }
      val static = (configs["$field.$VALUE"] as? String)?.takeIf { it.isNotBlank() }

      val sources = buildList {
        if (fromKey) add(FieldSource.FromKey)
        if (pointer != null) {
          val compiled =
              try {
                JsonPointer.compile(pointer)
              } catch (e: IllegalArgumentException) {
                throw IllegalArgumentException(
                    "restate.record.mapper.$field.$POINTER: ${e.message}",
                    e,
                )
              }
          add(FieldSource.FromPointer(compiled))
        }
        if (static != null) add(FieldSource.Static(static))
      }

      return when (sources.size) {
        0 -> {
          require(!required) {
            "restate.record.mapper.$field requires one of .$FROMKEY/.$POINTER/.$VALUE for ${javaClass.name}"
          }
          null
        }
        1 -> sources.single()
        else ->
            throw IllegalArgumentException(
                "restate.record.mapper.$field: set exactly one of .$FROMKEY/.$POINTER/.$VALUE"
            )
      }
    }
    service = source(SERVICE, required = true)!!
    handler = source(HANDLER, required = true)!!
    key = source(KEY, required = false)
    idempotencyKey = source(IDEMPOTENCY_KEY, required = false)
    scope = source(SCOPE, required = false)
    limitKey = source(LIMIT_KEY, required = false)
  }

  // Static fields become defaults on the stream Settings (the stream also stamps the producer id).
  override fun initialSettings(): StreamSettings {
    val builder = Settings.newBuilder()
    (service as? FieldSource.Static)?.let { builder.service = it.value }
    (handler as? FieldSource.Static)?.let { builder.handler = it.value }
    (scope as? FieldSource.Static)?.let { builder.scope = it.value }
    (limitKey as? FieldSource.Static)?.let { builder.limitKey = it.value }
    return builder.build()
  }

  override fun toInvocation(record: ConsumerRecord<String, JsonNode>): Invocation {
    val recordKey = record.key()
    val value = record.value()

    val builder =
        Record.newBuilder()
            .setOffset(record.offset())
            .setPayload(
                value?.let { ByteString.copyFrom(MAPPER.writeValueAsBytes(it)) } ?: ByteString.EMPTY
            )

    // service/handler/scope/limit_key: static ones are Settings defaults; set per-record only when
    // derived dynamically (from the key or a JSON pointer).
    if (service !is FieldSource.Static) {
      builder.service = extractRequired(service, SERVICE, recordKey, value, record.offset())
    }
    if (handler !is FieldSource.Static) {
      builder.handler = extractRequired(handler, HANDLER, recordKey, value, record.offset())
    }
    dynamicOverride(scope, recordKey, value)?.let { builder.scope = it }
    dynamicOverride(limitKey, recordKey, value)?.let { builder.limitKey = it }

    // key + idempotency key have no Settings field, so they are always per-record.
    key?.extract(recordKey, value)?.let { builder.key = it }
    idempotencyKey?.extract(recordKey, value)?.let { builder.idempotencyKey = it }

    for (header in record.headers()) {
      val v = header.value() ?: continue
      when (header.key()) {
        TRACEPARENT -> builder.traceparent = String(v, Charsets.UTF_8)
        TRACESTATE -> builder.tracestate = String(v, Charsets.UTF_8)
      }
    }

    return builder.build()
  }

  /**
   * A per-record override for an optional field: null when unset or static (a Settings default).
   */
  private fun dynamicOverride(source: FieldSource?, recordKey: String?, value: JsonNode?): String? =
      if (source == null || source is FieldSource.Static) null else source.extract(recordKey, value)

  private fun extractRequired(
      source: FieldSource,
      name: String,
      recordKey: String?,
      value: JsonNode?,
      offset: Long,
  ): String =
      source.extract(recordKey, value)
          ?: throw IllegalStateException(
              "record mapper could not extract required '$name' from the record at offset $offset"
          )

  /** How a single field is derived from a record. */
  private sealed interface FieldSource {
    fun extract(recordKey: String?, value: JsonNode?): String?

    data class Static(val value: String) : FieldSource {
      override fun extract(recordKey: String?, value: JsonNode?): String = this.value
    }

    object FromKey : FieldSource {
      override fun extract(recordKey: String?, value: JsonNode?): String? = recordKey
    }

    data class FromPointer(val pointer: JsonPointer) : FieldSource {
      override fun extract(recordKey: String?, value: JsonNode?): String? {
        val node = value?.at(pointer) ?: return null
        return if (node.isMissingNode || node.isNull) null else node.asText()
      }
    }
  }

  companion object {
    private const val SERVICE = "service"
    private const val HANDLER = "handler"
    private const val KEY = "key"
    private const val IDEMPOTENCY_KEY = "idempotencykey"
    private const val SCOPE = "scope"
    private const val LIMIT_KEY = "limitkey"
    private const val FROMKEY = "fromkey"
    private const val POINTER = "pointer"
    private const val VALUE = "value"
    private const val TRACEPARENT = "traceparent"
    private const val TRACESTATE = "tracestate"
    private val MAPPER = ObjectMapper()
  }
}

/** Deserializes Kafka record bytes into a Jackson [JsonNode]; a null (tombstone) stays null. */
class JsonNodeDeserializer : Deserializer<JsonNode> {
  override fun deserialize(topic: String?, data: ByteArray?): JsonNode? = data?.let {
    MAPPER.readTree(it)
  }

  companion object {
    private val MAPPER = ObjectMapper()
  }
}
