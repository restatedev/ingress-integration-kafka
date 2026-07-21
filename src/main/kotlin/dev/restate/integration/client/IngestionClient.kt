package dev.restate.integration.client

import io.vertx.core.Vertx

/** Opens `Ingest` streams against a Restate ingress endpoint. */
interface IngestionClient {

  suspend fun open(producerId: String, listener: IngestionStream.Listener): IngestionStream

  suspend fun close()

  companion object {
    /** Build a gRPC-backed client with a connection wired for [endpoint]'s transport. */
    fun connect(
        vertx: Vertx,
        endpoint: IngressEndpoint,
    ): IngestionClient = IngestionClientImpl.connect(vertx, endpoint)
  }
}
