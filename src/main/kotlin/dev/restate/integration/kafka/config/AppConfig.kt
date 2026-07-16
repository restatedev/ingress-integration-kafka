package dev.restate.integration.kafka.config

import dev.restate.integration.kafka.IngressEndpoint
import io.vertx.core.VertxOptions
import java.net.URI
import java.util.Properties

/**
 * Fully-resolved, validated runtime configuration.
 *
 * Built by merging an optional Kafka properties file with environment variables (env wins).
 * See [load]. Pure/host-agnostic so it is trivial to unit test with an injected env map. Invalid
 * configuration is reported via [IllegalArgumentException] (thrown by the `require*` checks) with a
 * user-facing, actionable message.
 */
data class AppConfig(
  /** Final Kafka consumer properties (Confluent-mapped env + file, with forced overrides). */
    val kafkaConsumerConfig: Map<String, String>,
  /** Topics this consumer group subscribes to (>= 1). */
    val topics: List<String>,
  /** Kafka consumer group id; also the leading component of every stream's producer_id. */
    val groupId: String,
  val ingress: IngressEndpoint,
  val targetService: String,
  val targetHandler: String,
  /** Number of consumer verticle instances == event-loop pool size. */
    val consumerInstances: Int,
) {
  companion object {
    /** Env prefix whose members become Kafka consumer properties. */
    const val KAFKA_PREFIX = "KAFKA_"

    // Reserved KAFKA_-prefixed env vars that are consumed by the app and NOT forwarded to Kafka.
    const val TOPICS_ENV = "KAFKA_TOPICS"

    const val INGRESS_URL_ENV = "RESTATE_INGRESS_URL"
    const val TARGET_SERVICE_ENV = "RESTATE_TARGET_SERVICE"
    const val TARGET_HANDLER_ENV = "RESTATE_TARGET_HANDLER"
    const val CONSUMER_INSTANCES_ENV = "RESTATE_KAFKA_CONSUMER_INSTANCES"
    const val CONFIG_FILE_ENV = "CONFIG_FILE"

    private val RESERVED_KAFKA_ENV = setOf(TOPICS_ENV)

    // Deserializers/commit mode we always control, regardless of user input: keys are UTF-8
    // strings (the VO key), values are opaque bytes (the payload), and offsets are committed
    // manually only after Restate confirms them.
    private val FORCED_KAFKA_CONFIG =
        mapOf(
            "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "enable.auto.commit" to "false",
        )


    fun load(): AppConfig {
      val fileConfig = loadPropertiesFile(System.getenv(CONFIG_FILE_ENV))
      return load(System.getenv(), fileConfig)
    }

    /**
     * Resolve configuration.
     *
     * @param env process environment (inject a map in tests)
     * @param fileConfig base Kafka properties from an optional properties file (env overrides these)
     */
    internal fun load(
        env: Map<String, String>,
        fileConfig: Map<String, String> = emptyMap(),
    ): AppConfig {
      // Map a `KAFKA_*` env var name to a Kafka property name using the Confluent Docker
      // convention: lower-cased, then a run of underscores collapses to a single separator by
      // length -- `_` -> `.`, `__` -> `_`, `___` (or more) -> `-`. Examples:
      // KAFKA_BOOTSTRAP_SERVERS -> bootstrap.servers, KAFKA_SASL_JAAS_CONFIG -> sasl.jaas.config,
      // KAFKA_FOO__BAR -> foo_bar, KAFKA_FOO___BAR -> foo-bar.
      fun envKeyToKafkaProp(envKey: String): String =
          envKey.removePrefix(KAFKA_PREFIX).lowercase().replace(Regex("_+")) { m ->
            when (m.value.length) {
              1 -> "."
              2 -> "_"
              else -> "-"
            }
          }

      // 1. Kafka consumer config: file props, overlaid by KAFKA_* env (minus reserved), then forced.
      val kafka = LinkedHashMap<String, String>()
      kafka.putAll(fileConfig)
      env.forEach { (key, value) ->
        if (key.startsWith(KAFKA_PREFIX) && key !in RESERVED_KAFKA_ENV) {
          kafka[envKeyToKafkaProp(key)] = value
        }
      }
      kafka.putAll(FORCED_KAFKA_CONFIG)

      require(!kafka["bootstrap.servers"].isNullOrBlank()) {
        "Missing Kafka 'bootstrap.servers'. Set KAFKA_BOOTSTRAP_SERVERS (or provide it in the config file)."
      }
      val groupId = kafka["group.id"]
      require(!groupId.isNullOrBlank()) {
        "Missing Kafka 'group.id'. Set KAFKA_GROUP_ID (or provide it in the config file)."
      }

      // 2. Topics.
      val topics =
          (env[TOPICS_ENV] ?: fileConfig[TOPICS_ENV])
              ?.split(',')
              ?.map { it.trim() }
              ?.filter { it.isNotEmpty() }
              ?: emptyList()
      require(topics.isNotEmpty()) {
        "Missing topics. Set $TOPICS_ENV to a comma-separated list, e.g. $TOPICS_ENV=orders,payments."
      }

      // 3. Restate wiring.
      val ingress = parseIngressUrl(env[INGRESS_URL_ENV])
      val targetService =
          requireNotNull(env[TARGET_SERVICE_ENV]?.trim()?.takeIf { it.isNotEmpty() }) {
            "Missing $TARGET_SERVICE_ENV (the Restate service to invoke)."
          }
      val targetHandler =
          requireNotNull(env[TARGET_HANDLER_ENV]?.trim()?.takeIf { it.isNotEmpty() }) {
            "Missing $TARGET_HANDLER_ENV (the Restate handler to invoke)."
          }

      // 4. Parallelism.
      val defaultInstances = VertxOptions.DEFAULT_EVENT_LOOP_POOL_SIZE
      val consumerInstances =
          env[CONSUMER_INSTANCES_ENV]?.trim()?.takeIf { it.isNotEmpty() }?.let { raw ->
            val n =
                requireNotNull(raw.toIntOrNull()) {
                  "$CONSUMER_INSTANCES_ENV must be an integer, got '$raw'."
                }
            require(n >= 1) { "$CONSUMER_INSTANCES_ENV must be >= 1, got $n." }
            n
          } ?: defaultInstances

      return AppConfig(
          kafkaConsumerConfig = kafka,
          topics = topics,
          groupId = groupId,
          ingress = ingress,
          targetService = targetService,
          targetHandler = targetHandler,
          consumerInstances = consumerInstances,
      )
    }

    private fun parseIngressUrl(raw: String?): IngressEndpoint {
      require(!raw.isNullOrBlank()) {
        "Missing $INGRESS_URL_ENV (the Restate ingestion endpoint, e.g. http://localhost:8080)."
      }
      val uri =
          try {
            URI(raw.trim())
          } catch (e: Exception) {
            throw IllegalArgumentException("$INGRESS_URL_ENV is not a valid URL: '$raw' (${e.message}).")
          }
      val tls =
          when (uri.scheme?.lowercase()) {
            "https" -> true
            "http" -> false
            else ->
                throw IllegalArgumentException(
                    "$INGRESS_URL_ENV must use http or https scheme, got '${uri.scheme}' in '$raw'.")
          }
      val host = requireNotNull(uri.host) { "$INGRESS_URL_ENV has no host: '$raw'." }
      val port = if (uri.port != -1) uri.port else if (tls) 443 else 80
      return IngressEndpoint(host, port, tls)
    }

    /** Load a `.properties` file into a plain map; returns empty if [path] is null/blank. */
   internal fun loadPropertiesFile(path: String?): Map<String, String> {
      if (path.isNullOrBlank()) return emptyMap()
      val file = java.io.File(path)
      require(file.isFile) { "$CONFIG_FILE_ENV points to a missing file: '$path'." }
      val props = Properties()
      file.inputStream().use { props.load(it) }
      return props.entries.associate { (k, v) -> k.toString() to v.toString() }
    }
  }
}
