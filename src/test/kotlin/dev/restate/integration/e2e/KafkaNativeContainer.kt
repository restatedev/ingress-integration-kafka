package dev.restate.integration.e2e

import com.github.dockerjava.api.command.InspectContainerResponse
import java.time.Duration
import org.testcontainers.images.builder.Transferable
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/** Copied from e2e repo */
class KafkaNativeContainer :
    KafkaContainer(
        DockerImageName.parse("docker.io/apache/kafka-native:4.1.1")
            .asCompatibleSubstituteFor("apache/kafka")
    ) {

  init {
    withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    withStartupTimeout(Duration.ofMinutes(2))
    withNetworkAliases("kafka")
    addExposedPort(EXTERNAL_PORT)
  }

  /** Bootstrap servers for clients on the host JVM (the EXTERNAL listener). */
  fun hostBootstrapServers(): String = "$host:${getMappedPort(EXTERNAL_PORT)}"

  override fun containerIsStarting(containerInfo: InspectContainerResponse) {
    // Don't call super: we own the listener configuration.
    val externalPort = getMappedPort(EXTERNAL_PORT)
    val advertisedListeners =
        listOf(
                "INTERNAL://kafka:$NETWORK_PORT",
                "EXTERNAL://$host:$externalPort",
                "BROKER://${containerInfo.config.hostName}:9093",
            )
            .joinToString(",")

    val command =
        """
      #!/bin/bash
      export KAFKA_ADVERTISED_LISTENERS=$advertisedListeners
      export KAFKA_LISTENERS=INTERNAL://0.0.0.0:$NETWORK_PORT,EXTERNAL://0.0.0.0:$EXTERNAL_PORT,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9095
      export KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      export KAFKA_INTER_BROKER_LISTENER_NAME=BROKER
      export KAFKA_CONTROLLER_QUORUM_VOTERS=1@${containerInfo.config.hostName}:9095
      /etc/kafka/docker/run
    """
            .trimIndent()

    copyFileToContainer(Transferable.of(command, 0x1ff), STARTER_SCRIPT)
  }

  companion object {
    const val NETWORK_PORT = 9092
    const val EXTERNAL_PORT = 9094
    const val STARTER_SCRIPT = "/tmp/testcontainers_start.sh"
  }
}
