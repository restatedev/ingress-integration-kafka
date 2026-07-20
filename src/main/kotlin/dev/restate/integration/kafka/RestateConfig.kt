package dev.restate.integration.kafka

import dev.restate.integration.client.IngressEndpoint
import dev.restate.integration.client.RetryPolicy
import io.vertx.core.VertxOptions
import java.net.URI
import kotlin.time.Duration.Companion.milliseconds
import org.apache.kafka.common.Configurable
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Range
import org.apache.kafka.common.config.ConfigDef.Type

/**
 * The Restate/application options, described with Kafka's [ConfigDef] so they get the same typed
 * parsing, defaults, range validation and unknown-key warnings as any Kafka client config — the
 * "Kafka way", reusing the same machinery as [org.apache.kafka.clients.consumer.ConsumerConfig].
 *
 * [AppConfig.load] builds the source map (properties file overlaid by env) and constructs this; the
 * keys are the `restate.*` file keys (plus `topics`), mapped from `RESTATE_*` (and `KAFKA_TOPICS`)
 * in the environment. Typed accessors below expose the parsed values; bad types/ranges surface as
 * [org.apache.kafka.common.config.ConfigException] (wrapped by [AppConfig.load]).
 *
 * The record mapper is loaded the "Kafka way": [recordMapper] instantiates the class named by
 * `restate.record.mapper.class` (default [StaticRecordMapper]) and, if it is [Configurable], hands
 * it its `restate.record.mapper.*` sub-config.
 */
class RestateConfig(props: Map<String, String>) :
    AbstractConfig(CONFIG_DEF, props, /* doLog= */ false) {

  val ingress: IngressEndpoint = parseIngressUrl(getString(INGRESS_URL))
  val topics: List<String> = getList(TOPICS).map { it.trim() }.filter { it.isNotEmpty() }
  val consumerInstances: Int = getInt(CONSUMER_INSTANCES)
  val retryPolicy: RetryPolicy =
      RetryPolicy(
          initialInterval = getLong(RETRY_INITIAL_INTERVAL_MS).milliseconds,
          exponentiationFactor = getDouble(RETRY_EXPONENTIATION_FACTOR),
          maxInterval = getLong(RETRY_MAX_INTERVAL_MS).milliseconds,
          maxAttempts = getInt(RETRY_MAX_ATTEMPTS),
      )

  /**
   * Instantiate the configured [RecordMapper] (`restate.record.mapper.class`, default
   * [StaticRecordMapper]) and configure it with its `restate.record.mapper.*` sub-config.
   */
  fun recordMapper(): RecordMapper<*, *> {
    val cls = getClass(RECORD_MAPPER_CLASS)
    val instance = cls.getDeclaredConstructor().newInstance()
    require(instance is RecordMapper<*, *>) {
      "$RECORD_MAPPER_CLASS must implement ${RecordMapper::class.java.name}, got ${cls.name}"
    }
    if (instance is Configurable) instance.configure(originalsWithPrefix(RECORD_MAPPER_PREFIX))
    return instance
  }

  companion object {
    const val INGRESS_URL = "restate.ingress.url"
    const val TOPICS = "topics"
    const val CONSUMER_INSTANCES = "restate.kafka.consumer.instances"
    const val RECORD_MAPPER_CLASS = "restate.record.mapper.class"
    const val RECORD_MAPPER_PREFIX = "restate.record.mapper."
    const val RETRY_INITIAL_INTERVAL_MS = "restate.retry.initial.interval.ms"
    const val RETRY_MAX_INTERVAL_MS = "restate.retry.max.interval.ms"
    const val RETRY_EXPONENTIATION_FACTOR = "restate.retry.exponentiation.factor"
    const val RETRY_MAX_ATTEMPTS = "restate.retry.max.attempts"

    private val RETRY_DEFAULTS = RetryPolicy()

    val CONFIG_DEF: ConfigDef =
        ConfigDef()
            .define(
                INGRESS_URL,
                Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                object : ConfigDef.Validator {
                  override fun ensureValid(name: String, value: Any?) {
                    checkNotNull(value) { "Missing $name" }
                    require(value is String) {
                      "Expected $name to be a string, got ${value.javaClass.name}"
                    }
                    parseIngressUrl(value)
                  }

                  override fun toString(): String {
                    return "valid ingress url"
                  }
                },
                Importance.HIGH,
                "Restate ingestion endpoint, e.g. http://localhost:8080 (https => TLS). Env: RESTATE_INGRESS_URL.",
            )
            .define(
                TOPICS,
                Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidList.anyNonDuplicateValues(false, false),
                Importance.HIGH,
                "Comma-separated Kafka topics to subscribe to. Env: KAFKA_TOPICS.",
            )
            .define(
                RECORD_MAPPER_CLASS,
                Type.CLASS,
                StaticRecordMapper::class.java,
                Importance.MEDIUM,
                "RecordMapper implementation to load. Env: RESTATE_RECORD_MAPPER_CLASS. Its own config lives under restate.record.mapper.* (RESTATE_RECORD_MAPPER_*).",
            )
            .define(
                CONSUMER_INSTANCES,
                Type.INT,
                VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE,
                Range.atLeast(1),
                Importance.MEDIUM,
                "Consumer verticle instances (one KafkaConsumer per event loop). Env: RESTATE_KAFKA_CONSUMER_INSTANCES.",
            )
            .define(
                RETRY_INITIAL_INTERVAL_MS,
                Type.LONG,
                RETRY_DEFAULTS.initialInterval.inWholeMilliseconds,
                Range.atLeast(1),
                Importance.LOW,
                "Initial reconnect backoff, in milliseconds. Env: RESTATE_RETRY_INITIAL_INTERVAL_MS.",
            )
            .define(
                RETRY_MAX_INTERVAL_MS,
                Type.LONG,
                RETRY_DEFAULTS.maxInterval.inWholeMilliseconds,
                Range.atLeast(1),
                Importance.LOW,
                "Maximum reconnect backoff, in milliseconds. Env: RESTATE_RETRY_MAX_INTERVAL_MS.",
            )
            .define(
                RETRY_EXPONENTIATION_FACTOR,
                Type.DOUBLE,
                RETRY_DEFAULTS.exponentiationFactor,
                Importance.LOW,
                "Reconnect backoff multiplier (>= 1.0, enforced by RetryPolicy). Env: RESTATE_RETRY_EXPONENTIATION_FACTOR.",
            )
            // Optional: null default = retry forever. RetryPolicy enforces >= 1 when set.
            .define(
                RETRY_MAX_ATTEMPTS,
                Type.INT,
                null,
                Importance.LOW,
                "Max consecutive reconnect attempts before giving up; unset = retry forever. Env: RESTATE_RETRY_MAX_ATTEMPTS.",
            )

    private fun parseIngressUrl(raw: String): IngressEndpoint {
      val uri =
          try {
            URI(raw.trim())
          } catch (e: Exception) {
            throw IllegalArgumentException(
                "RESTATE_INGRESS_URL is not a valid URL: '$raw' (${e.message})."
            )
          }
      val tls =
          when (uri.scheme?.lowercase()) {
            "https" -> true
            "http" -> false
            else ->
                throw IllegalArgumentException(
                    "RESTATE_INGRESS_URL must use http or https scheme, got '${uri.scheme}' in '$raw'."
                )
          }
      val host = requireNotNull(uri.host) { "RESTATE_INGRESS_URL has no host: '$raw'." }
      val port = if (uri.port != -1) uri.port else if (tls) 443 else 80
      return IngressEndpoint(host, port, tls)
    }
  }
}
