package dev.restate.integration.e2e

import dev.restate.integration.kafka.AppConfig
import dev.restate.integration.kafka.ConsumerVerticle
import io.vertx.core.Vertx
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/**
 * Full end-to-end, all off published images (no local build required):
 * - `Kafka` (apache/kafka)
 * - `Restate` (`ghcr.io/restatedev/restate:main`, assumed to carry the ingestion-API feature)
 * - the Java SDK `test-services` image running only the `Counter` virtual object
 * - our [ConsumerVerticle], in-process, consuming from Kafka and streaming into Restate's ingress.
 *
 * `Counter` is the oracle: we produce values `[1..5]` under one key to a single partition and point
 * ingestion at `Counter/add`. Asserting `Counter.get(key) == sum` in one shot proves **delivery**,
 * **Kafka-key → virtual-object-key mapping**, and **exactly-once** (no double-add on the pull-based
 * window/commit path). Restate reaches the services container over the shared network by alias.
 *
 * Skips (not fails) when Docker isn't available, so `./gradlew build` stays green on CI without it.
 */
class KafkaToRestateE2ETest {

  companion object {
    private const val RESTATE_IMAGE = "ghcr.io/restatedev/restate:main"
    private const val TEST_SERVICES_IMAGE = "ghcr.io/restatedev/test-services-java:main"
    private const val INGRESS_PORT = 8080
    private const val ADMIN_PORT = 9070
    private const val SERVICES_PORT = 9080
    private const val TOPIC = "test-topic"
    private const val KEY = "user-1"
    private val VALUES = listOf(1L, 2L, 3L, 4L, 5L)
  }

  @Test
  @Timeout(value = 6, unit = TimeUnit.MINUTES)
  fun `kafka records accumulate into a restate virtual object exactly once`() {
    val network = Network.newNetwork()
    val kafka = KafkaNativeContainer().apply { withNetwork(network) }
    val restate =
        GenericContainer(DockerImageName.parse(RESTATE_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("restate")
            .withExposedPorts(INGRESS_PORT, ADMIN_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(ADMIN_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(2.minutes.toJavaDuration())
            )
    val services =
        GenericContainer(DockerImageName.parse(TEST_SERVICES_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("test-services")
            .withEnv("SERVICES", "Counter")
            .withExposedPorts(SERVICES_PORT)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(1.minutes.toJavaDuration()))

    var vertx: Vertx? = null
    try {
      kafka.start()
      services.start()
      restate.start()

      val adminBase = "http://${restate.host}:${restate.getMappedPort(ADMIN_PORT)}"
      val ingressBase = "http://${restate.host}:${restate.getMappedPort(INGRESS_PORT)}"

      // Restate reaches the services container over the shared network by alias.
      registerDeployment(adminBase, "http://test-services:$SERVICES_PORT")

      // Produce the batch before the consumer exists; a fresh group + earliest replays it.
      createTopic(kafka.hostBootstrapServers(), TOPIC)
      VALUES.forEach { produce(kafka.hostBootstrapServers(), TOPIC, KEY, it.toString()) }

      vertx = Vertx.vertx()
      val config =
          AppConfig.load(
              mapOf(
                  "KAFKA_BOOTSTRAP_SERVERS" to kafka.hostBootstrapServers(),
                  "KAFKA_GROUP_ID" to "e2e",
                  "KAFKA_TOPICS" to TOPIC,
                  "KAFKA_AUTO_OFFSET_RESET" to "earliest",
                  "RESTATE_INGRESS_URL" to ingressBase,
                  "RESTATE_RECORD_MAPPER_SERVICE" to "Counter",
                  "RESTATE_RECORD_MAPPER_HANDLER" to "add",
                  "RESTATE_KAFKA_CONSUMER_INSTANCES" to "1",
              )
          )
      vertx
          .deployVerticle(ConsumerVerticle(config))
          .toCompletionStage()
          .toCompletableFuture()
          .get(30, TimeUnit.SECONDS)

      val expected = VALUES.sum()
      awaitAsserted(2.minutes) {
        assertThat(counterGet(ingressBase, KEY)).isEqualTo(expected)
      }
    } finally {
      vertx?.close()?.toCompletionStage()?.toCompletableFuture()?.get(10, TimeUnit.SECONDS)
      restate.stop()
      services.stop()
      kafka.stop()
      network.close()
    }
  }

  // ---- helpers ----

  private val http = HttpClient.newHttpClient()

  private fun registerDeployment(adminBase: String, serviceUri: String) {
    val request =
        HttpRequest.newBuilder(URI.create("$adminBase/deployments"))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("""{"uri":"$serviceUri"}"""))
            .build()
    val response = http.send(request, HttpResponse.BodyHandlers.ofString())
    check(response.statusCode() in 200..299) {
      "deployment registration failed (${response.statusCode()}): ${response.body()}"
    }
  }

  /**
   * Invoke the shared `Counter.get(key)` handler through the ingress; null if not yet answerable.
   */
  private fun counterGet(ingressBase: String, key: String): Long? {
    val request =
        HttpRequest.newBuilder(URI.create("$ingressBase/Counter/$key/get"))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()
    return runCatching {
          val response = http.send(request, HttpResponse.BodyHandlers.ofString())
          if (response.statusCode() in 200..299) response.body().trim().toLongOrNull() else null
        }
        .getOrNull()
  }

  private fun createTopic(bootstrap: String, topic: String) {
    Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap)).use { admin ->
      admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get(30, TimeUnit.SECONDS)
    }
  }

  private fun produce(bootstrap: String, topic: String, key: String, value: String) {
    val props =
        Properties().apply {
          put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
          put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
          put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
        }
    KafkaProducer<String, ByteArray>(props).use { producer ->
      producer.send(ProducerRecord(topic, key, value.toByteArray())).get(30, TimeUnit.SECONDS)
    }
  }

  /**
   * Run [assertion] on the polling interval until it passes; if it never does within [timeout],
   * rethrow the last [AssertionError] so the failure carries AssertJ's expected/actual detail.
   */
  private fun awaitAsserted(timeout: Duration, assertion: () -> Unit) {
    val deadline = Clock.System.now() + timeout
    while (true) {
      try {
        assertion()
        return
      } catch (e: AssertionError) {
        if (Clock.System.now() >= deadline) throw e
        Thread.sleep(1000)
      }
    }
  }
}
