package dev.restate.integration.client

import io.vertx.core.Vertx

/** Opens `Ingest` streams against a Restate ingress endpoint. */
interface IntegrationClient {

  suspend fun open(
      producerId: String,
      listener: InvocationStream.Listener,
      initialStreamDefaults: StreamDefaults,
  ): InvocationStream

  suspend fun close()

  companion object {
    /** Build a client with a connection wired for [endpoints]'s transport. */
    fun connect(
        vertx: Vertx,
        endpoints: List<IngressEndpoint>,
    ): IntegrationClient = IntegrationClientImpl.connect(vertx, endpoints)
  }
}
