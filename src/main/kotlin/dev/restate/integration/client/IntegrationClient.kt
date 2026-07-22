package dev.restate.integration.client

import io.vertx.core.Vertx

/** Opens `Ingest` streams against a Restate ingress endpoint. */
interface IntegrationClient {

  suspend fun open(
      producerId: String,
      listener: InvocationStream.Listener,
      initialStreamSettings: StreamSettings,
  ): InvocationStream

  suspend fun close()

  companion object {
    /** Build a client with a connection wired for [endpoint]'s transport. */
    fun connect(
        vertx: Vertx,
        endpoint: IngressEndpoint,
    ): IntegrationClient = IntegrationClientImpl.connect(vertx, endpoint)
  }
}
