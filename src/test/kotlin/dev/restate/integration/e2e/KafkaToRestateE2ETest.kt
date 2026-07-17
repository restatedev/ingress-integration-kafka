package dev.restate.integration.e2e

import dev.restate.integration.kafka.AppConfig
import dev.restate.integration.kafka.ConsumerVerticle
import io.vertx.core.Vertx
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Full end-to-end: produce a Kafka record and prove it reaches a real handler invoked by a real,
 * branch-built Restate.
 *
 * Topology (all on one Testcontainers network): `Kafka` + `Restate` (the ingestion-API branch
 * binary baked into a Fedora image) + a `Greeter` Restate SDK service (the `:e2e-greeter` module
 * image). Our [ConsumerVerticle] runs in-process, consuming from Kafka and streaming into Restate's
 * ingress. We register the Greeter with Restate, produce a record, and assert the Greeter container
 * logged it.
 *
 * Skips (rather than fails) when the local prerequisites are absent, so `./gradlew build` stays
 * green without the sibling `../restate` checkout:
 * - the branch `restate-server` binary (env `RESTATE_SERVER_BINARY`, or the default path), and
 * - the `restate-e2e-greeter:test` image (`./gradlew :e2e-greeter:jibDockerBuild`).
 */
class KafkaToRestateE2ETest {

  @Test
  @Timeout(value = 6, unit = TimeUnit.MINUTES)
  fun `a kafka record reaches the registered restate handler`() {
    val binary = Path.of(System.getenv("RESTATE_SERVER_BINARY") ?: DEFAULT_BINARY)
    assumeTrue(Files.isRegularFile(binary)) {
      "restate-server binary not found at $binary; skipping e2e"
    }
    assumeTrue(imageExists(GREETER_IMAGE)) {
      "$GREETER_IMAGE not built; run ./gradlew :e2e-greeter:jibDockerBuild"
    }

    val network = Network.newNetwork()
    val kafka = KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withNetwork(network)
    val restate =
        GenericContainer(
                ImageFromDockerfile()
                    .withFileFromPath("restate-server", binary)
                    .withFileFromString("Dockerfile", RESTATE_DOCKERFILE)
            )
            .withNetwork(network)
            .withNetworkAliases("restate")
            .withExposedPorts(8080, 9070)
            .withCommand("--base-dir", "/tmp/restate")
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(9070)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(150))
            )
    val greeter =
        GenericContainer(DockerImageName.parse(GREETER_IMAGE))
            .withNetwork(network)
            .withNetworkAliases("greeter")
            .withExposedPorts(9080)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))

    var vertx: Vertx? = null
    try {
      kafka.start()
      greeter.start()
      restate.start()

      // Restate reaches the Greeter over the shared network by alias.
      registerDeployment(
          adminBase = "http://${restate.host}:${restate.getMappedPort(9070)}",
          serviceUri = "http://greeter:9080",
      )

      createTopic(kafka.bootstrapServers, TOPIC)
      produce(kafka.bootstrapServers, TOPIC, key = "user-1", value = PAYLOAD)

      vertx = Vertx.vertx()
      val config =
          AppConfig.load(
              mapOf(
                  "KAFKA_BOOTSTRAP_SERVERS" to kafka.bootstrapServers,
                  "KAFKA_GROUP_ID" to "e2e",
                  "KAFKA_TOPICS" to TOPIC,
                  "KAFKA_AUTO_OFFSET_RESET" to "earliest",
                  "RESTATE_INGRESS_URL" to "http://${restate.host}:${restate.getMappedPort(8080)}",
                  "RESTATE_TARGET_SERVICE" to "Greeter",
                  "RESTATE_TARGET_HANDLER" to "greet",
                  "RESTATE_KAFKA_CONSUMER_INSTANCES" to "1",
              )
          )
      vertx
          .deployVerticle(ConsumerVerticle(config))
          .toCompletionStage()
          .toCompletableFuture()
          .get(30, TimeUnit.SECONDS)

      val delivered =
          awaitUntil(Duration.ofSeconds(120)) { "GREETER_RECEIVED:$PAYLOAD" in greeter.logs }
      assertThat(delivered)
          .withFailMessage { "Greeter never logged the record. Greeter logs:\n${greeter.logs}" }
          .isTrue()
    } finally {
      vertx?.close()?.toCompletionStage()?.toCompletableFuture()?.get(10, TimeUnit.SECONDS)
      restate.stop()
      greeter.stop()
      kafka.stop()
      network.close()
    }
  }

  // ---- helpers ----

  private fun imageExists(name: String): Boolean =
      try {
        ProcessBuilder("docker", "image", "inspect", name)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()
            .waitFor() == 0
      } catch (e: Exception) {
        false
      }

  private fun registerDeployment(adminBase: String, serviceUri: String) {
    val client = HttpClient.newHttpClient()
    val request =
        HttpRequest.newBuilder(URI.create("$adminBase/deployments"))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("""{"uri":"$serviceUri"}"""))
            .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    check(response.statusCode() in 200..299) {
      "deployment registration failed (${response.statusCode()}): ${response.body()}"
    }
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

  private fun awaitUntil(timeout: Duration, condition: () -> Boolean): Boolean {
    val deadline = System.nanoTime() + timeout.toNanos()
    while (System.nanoTime() < deadline) {
      if (condition()) return true
      Thread.sleep(1000)
    }
    return condition()
  }

  companion object {
    private const val DEFAULT_BINARY =
        "/home/slinkydeveloper/projects/work/restate/target/debug/restate-server"
    private const val GREETER_IMAGE = "restate-e2e-greeter:test"
    private const val KAFKA_IMAGE = "apache/kafka:3.8.0"
    private const val TOPIC = "test-topic"
    private const val PAYLOAD = "hello-restate"
    private val RESTATE_DOCKERFILE =
        """
        FROM fedora:43
        COPY restate-server /usr/local/bin/restate-server
        WORKDIR /
        ENTRYPOINT ["/usr/local/bin/restate-server"]
        """
            .trimIndent()
  }
}
