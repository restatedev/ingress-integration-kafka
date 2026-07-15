package dev.restate.integration.kafka

import dev.restate.ingestion.v1.IngestionSvcGrpcClient
import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Request
import dev.restate.ingestion.v1.Response
import dev.restate.ingestion.v1.Settings
import dev.restate.integration.kafka.config.IngressEndpoint
import io.vertx.core.Future
import io.vertx.core.net.SocketAddress
import io.vertx.grpc.client.GrpcClient
import io.vertx.grpc.client.GrpcClientRequest
import io.vertx.kotlin.coroutines.coAwait

/** Callbacks for the server->client half of an ingestion stream. Invoked on the stream's context. */
interface IngestionStreamListener {
  /** A flow-control window grant: the client may send [increment] more records (may be 0 = pure ack). */
  fun onWindowUpdate(increment: Long)

  /** Restate durably committed up to (and including) offset [lastCommitted]. */
  fun onCommit(lastCommitted: Long)

  /** Server reported an application error for this stream. */
  fun onError(error: dev.restate.ingestion.v1.Error)

  /** The response stream ended ([cause] null) or failed ([cause] non-null). */
  fun onClose(cause: Throwable?)
}

/**
 * A single open `Ingest` bidi stream == one Restate producer == one (topic, partition).
 *
 * Wraps the underlying [GrpcClientRequest] write side and translates incoming [Response] messages
 * into [IngestionStreamListener] callbacks.
 */
class IngestionStream
internal constructor(private val request: GrpcClientRequest<Request, Response>) {

  /** Send the stream [Settings] (producer_id, default target/headers). Send once, before records. */
  fun sendSettings(settings: Settings): Future<Void> =
      request.write(Request.newBuilder().setSettings(settings).build())

  /** Send a single [Record]. Caller must respect the flow-control window. */
  fun sendRecord(record: Record): Future<Void> =
      request.write(Request.newBuilder().setRecord(record).build())

  /** True when the write buffer is full; pair with [drainHandler] for backpressure. */
  fun writeQueueFull(): Boolean = request.writeQueueFull()

  fun drainHandler(handler: () -> Unit) {
    request.drainHandler { handler() }
  }

  /** Half-close the client side of the stream (we're done sending). */
  fun end(): Future<Void> = request.end()
}

/** Opens ingestion streams against a Restate ingress endpoint over a shared [GrpcClient]. */
class IngestionClient(private val grpcClient: GrpcClient, endpoint: IngressEndpoint) {

  private val address: SocketAddress =
      SocketAddress.inetSocketAddress(endpoint.port, endpoint.host)

  /**
   * Open a new bidirectional `Ingest` stream and wire its responses to [listener].
   *
   * The returned stream is ready for [IngestionStream.sendSettings]; the first
   * [IngestionStreamListener.onWindowUpdate] gates when records may start flowing.
   */
  suspend fun open(listener: IngestionStreamListener): IngestionStream {
    val request = grpcClient.request(address, IngestionSvcGrpcClient.Ingest).coAwait()
    request.response().onComplete { ar ->
      if (ar.succeeded()) {
        val response = ar.result()
        response.handler { dispatch(it, listener) }
        response.endHandler { listener.onClose(null) }
        response.exceptionHandler { listener.onClose(it) }
      } else {
        listener.onClose(ar.cause())
      }
    }
    return IngestionStream(request)
  }

  private fun dispatch(response: Response, listener: IngestionStreamListener) {
    // A Response may carry a commit watermark alongside its ack/error; surface the commit first.
    if (response.hasLastCommitted()) {
      listener.onCommit(response.lastCommitted)
    }
    when {
      response.hasAck() -> listener.onWindowUpdate(response.ack.increment)
      response.hasError() -> listener.onError(response.error)
    }
  }
}
