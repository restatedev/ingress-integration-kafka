package dev.restate.integration.kafka

import java.io.File
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigException

/**
 * Loads and validates configuration from a `.properties` file overlaid by environment variables
 * (env wins), then hands back the pieces the app runs on — nothing more:
 * - [kafkaConsumerConfig]: the Kafka consumer passthrough (native dotted keys in the file,
 *   `KAFKA_*` in the env), validated by Kafka's [ConsumerConfig];
 * - [restate]: the Restate/app options as a [RestateConfig] (`restate.*`/`topics` keys,
 *   `RESTATE_*`/ `KAFKA_TOPICS` env);
 * - [recordMapper]: the configured [RecordMapper], instantiated eagerly here so a bad mapper config
 *   fails at startup.
 */
data class AppConfig(
    val kafkaConsumerConfig: Map<String, String>,
    val restate: RestateConfig,
    val recordMapper: RecordMapper<Any?, Any?>,
) {
  /** Kafka consumer group id; also the leading component of every stream's producer_id. */
  val groupId: String
    get() = kafkaConsumerConfig.getValue("group.id")

  companion object {
    /**
     * Env prefix whose members become Kafka consumer properties. Properties key are upper level.
     */
    const val KAFKA_PREFIX = "KAFKA_"

    const val TOPICS_ENV = "KAFKA_TOPICS"
    const val CONFIG_FILE_ENV = "CONFIG_FILE"

    // Any `RESTATE_*` env var becomes a `restate.*` config key
    private const val RESTATE_ENV_PREFIX = "RESTATE_"
    private const val RESTATE_FILE_PREFIX = "restate."

    // The only commit mode we always control: offsets are committed manually, after Restate
    // confirms. The key/value deserializers are the mapper's business, not ours (see load()).
    private val FORCED_KAFKA_CONFIG = mapOf("enable.auto.commit" to "false")

    // Confluent Docker env convention: lower-cased, then a run of underscores collapses by length
    // --
    // `_` -> `.`, `__` -> `_`, `___` (or more) -> `-`. Shared by KAFKA_* (after its prefix is
    // stripped) and RESTATE_* keys, so both support literal `_`/`-` in property names.
    private fun envKeyToPropKey(s: String): String =
        s.lowercase().replace(Regex("_+")) { m ->
          when (m.value.length) {
            1 -> "."
            2 -> "_"
            else -> "-"
          }
        }

    fun load(): AppConfig {
      val fileConfig = loadPropertiesFiles(System.getenv(CONFIG_FILE_ENV))
      return load(System.getenv(), fileConfig)
    }

    /**
     * Resolve configuration.
     *
     * @param env process environment (inject a map in tests)
     * @param fileConfig base properties from an optional properties file (env overrides these)
     */
    internal fun load(
        env: Map<String, String>,
        fileConfig: Map<String, String> = emptyMap(),
    ): AppConfig {
      // 1. Restate options + record mapper. Built first (and eagerly, so a bad mapper config fails
      //    here, not at deploy) because the mapper dictates the Kafka key/value deserializers.
      //    `restate.*` (+ `topics`) file keys, overlaid by RESTATE_*/KAFKA_TOPICS env (env wins).
      val restateProps = LinkedHashMap<String, String>()
      fileConfig.forEach { (key, value) ->
        if (key.startsWith(RESTATE_FILE_PREFIX) || key == RestateConfig.TOPICS)
            restateProps[key] = value
      }
      env.forEach { (key, value) ->
        when {
          key == TOPICS_ENV -> restateProps[RestateConfig.TOPICS] = value
          key.startsWith(RESTATE_ENV_PREFIX) -> restateProps[envKeyToPropKey(key)] = value
        }
      }

      // 2. Kafka consumer passthrough: file keys that are NOT `restate.*`/`topics`, overlaid by
      //    KAFKA_* env (Confluent transform). The mapper dictates the deserializers; we force only
      //    manual commit on top.
      val kafka = LinkedHashMap<String, String>()
      fileConfig.forEach { (key, value) ->
        if (!key.startsWith(RESTATE_FILE_PREFIX) && key != RestateConfig.TOPICS) kafka[key] = value
      }
      env.forEach { (key, value) ->
        if (key.startsWith(KAFKA_PREFIX) && key != TOPICS_ENV) {
          kafka[envKeyToPropKey(key.removePrefix(KAFKA_PREFIX))] = value
        }
      }

      // Prepare restate configuration
      val restate =
          try {
            RestateConfig(restateProps)
          } catch (e: ConfigException) {
            throw IllegalArgumentException("Invalid Restate configuration: ${e.message}", e)
          }

      // Prepare record mapper, it will specify which deserializers to use.
      val recordMapper = restate.recordMapper()

      kafka["key.deserializer"] = recordMapper.keyDeserializer.name
      kafka["value.deserializer"] = recordMapper.valueDeserializer.name
      kafka.putAll(FORCED_KAFKA_CONFIG)

      // Couple of additional validation for Kafka.
      require(!kafka["bootstrap.servers"].isNullOrBlank()) {
        "Missing Kafka 'bootstrap.servers'. Set KAFKA_BOOTSTRAP_SERVERS (or provide it in the config file)."
      }
      require(!kafka["group.id"].isNullOrBlank()) {
        "Missing Kafka 'group.id'. Set KAFKA_GROUP_ID (or provide it in the config file)."
      }

      @Suppress("UNCHECKED_CAST")
      return AppConfig(
          kafkaConsumerConfig = kafka,
          restate = restate,
          recordMapper = recordMapper as RecordMapper<Any?, Any?>,
      )
    }

    /**
     * Load one or more `.properties` files into a single map. [paths] is a comma-separated list of
     * file paths (whitespace around each is trimmed, blank entries skipped); on a key collision a
     * later file wins over an earlier one. Returns empty if [paths] is null/blank.
     */
    internal fun loadPropertiesFiles(paths: String?): Map<String, String> {
      if (paths.isNullOrBlank()) return emptyMap()
      val merged = LinkedHashMap<String, String>()
      paths
          .split(',')
          .map { it.trim() }
          .filter { it.isNotEmpty() }
          .forEach { path ->
            val file = File(path)
            require(file.isFile) { "$CONFIG_FILE_ENV points to a missing file: '$path'." }
            val props = Properties()
            file.inputStream().use { props.load(it) }
            props.forEach { (k, v) -> merged[k.toString()] = v.toString() }
          }
      return merged
    }
  }
}
