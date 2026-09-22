package dev.restate.integration.kafka.ingestion

import dev.restate.integration.ExactlyOnceProducer
import dev.restate.integration.GrpcIntegrationClient
import dev.restate.integration.IntegrationClient
import dev.restate.integration.ProducerOptions
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpVersion
import io.vertx.core.net.SocketAddress
import io.vertx.grpcio.client.GrpcIoClient
import io.vertx.grpcio.client.GrpcIoClientChannel
import java.util.concurrent.atomic.AtomicInteger
import org.apache.logging.log4j.LogManager

/**
 * Multi endpoint [ProducerFactory] using Vert.x gRPC client.
 */
@Suppress("UnstableApiUsage")
class VertxMultiEndpointProducerFactory(
    vertx: Vertx,
    endpoints: List<IngressEndpoint>,
    integrationName: String,
    integrationVersion: String,
) : ProducerFactory, AutoCloseable {

  private val log = LogManager.getLogger(VertxMultiEndpointProducerFactory::class.java)

  private val transports: List<GrpcIoClient> = endpoints.map { endpoint ->
    buildTransport(vertx, endpoint)
  }

  private val clients: List<IntegrationClient> = endpoints.mapIndexed { i, endpoint ->
    val channel =
        GrpcIoClientChannel(
            transports[i],
            SocketAddress.inetSocketAddress(endpoint.port, endpoint.host),
        )
    GrpcIntegrationClient.builder(channel)
        .integration(integrationName, integrationVersion)
        .apply { endpoint.authToken?.let { authToken(it) } }
        .build()
  }

  private val cursor = AtomicInteger(0)

  private fun buildTransport(vertx: Vertx, endpoint: IngressEndpoint): GrpcIoClient {
    val options =
        HttpClientOptions()
            .setProtocolVersion(HttpVersion.HTTP_2)
            .setDefaultHost(endpoint.host)
            .setDefaultPort(endpoint.port)
    if (endpoint.tls) {
      options.setSsl(true).isUseAlpn = true
    } else {
      // Plaintext gRPC uses HTTP/2 with prior knowledge (no h2c upgrade dance).
      options.setHttp2ClearTextUpgrade(false)
    }
    return GrpcIoClient.client(vertx, options)
  }

  override fun newExactlyOnceProducer(
      producerId: String,
      options: ProducerOptions,
  ): ExactlyOnceProducer {
    val client = clients[cursor.getAndIncrement().mod(clients.size)]
    return client.newExactlyOnceProducer(producerId, options)
  }

  override fun close() {
    clients.forEach {
      try {
        it.close()
      } catch (e: Exception) {
        log.warn("error closing ingestion client", e)
      }
    }
    transports.forEach {
      try {
        it.close()
      } catch (e: Exception) {
        log.warn("error closing gRPC transport", e)
      }
    }
  }
}
