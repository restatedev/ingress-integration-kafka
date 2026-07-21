package dev.restate.integration.client

import dev.restate.ingestion.v1.Error
import dev.restate.ingestion.v1.ErrorKind
import dev.restate.ingestion.v1.IngestionSvcGrpcClient
import dev.restate.ingestion.v1.Response
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpVersion
import io.vertx.grpc.client.GrpcClient
import io.vertx.kotlin.coroutines.coAwait

internal class IngestionClientImpl(
    private val grpcClient: GrpcClient,
    endpoint: IngressEndpoint,
) : IngestionClient {

  private val authToken = endpoint.authToken

  override suspend fun open(
      producerId: String,
      listener: IngestionStream.Listener,
  ): IngestionStream {
    val request = grpcClient.request(IngestionSvcGrpcClient.Ingest).coAwait()
    if (authToken != null) {
      request.headers().set("Authorization", "Bearer $authToken")
    }

    val stream =
        IngestionStreamImpl(
            request,
            producerId,
            listener,
        )
    request.response().onComplete { ar ->
      if (ar.succeeded()) {
        val response = ar.result()
        response.handler { dispatch(it, stream, listener) }
        response.endHandler { listener.onClose(null) }
        response.exceptionHandler {
          listener.onClose(
              IngestionStreamException(
                  IngestionStreamException.Kind.UNKNOWN,
                  "got exception from response",
                  it,
              )
          )
        }
      } else {
        listener.onClose(
            IngestionStreamException(
                IngestionStreamException.Kind.UNKNOWN,
                "could not send the request",
                ar.cause(),
            )
        )
      }
    }
    return stream
  }

  override suspend fun close() {
    grpcClient.close().coAwait()
  }

  private fun dispatch(
      response: Response,
      stream: IngestionStreamImpl,
      listener: IngestionStream.Listener,
  ) {
    // A Response may carry an ack watermark alongside its window/error; surface the ack first.
    if (response.hasLastCommitted()) {
      listener.ack(response.lastCommitted)
    }
    when {
      // The window grant is flow control, not an application event: feed it to the stream's budget.
      response.hasAck() -> stream.grantWindow(response.ack.increment)
      response.hasError() -> listener.onClose(response.error.toStreamException())
    }
  }

  companion object {
    fun connect(
        vertx: Vertx,
        endpoint: IngressEndpoint,
    ): IngestionClientImpl =
        IngestionClientImpl(
            buildGrpcClient(vertx, endpoint),
            endpoint,
        )

    private fun buildGrpcClient(vertx: Vertx, endpoint: IngressEndpoint): GrpcClient {
      val httpOptions = HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)
      httpOptions.setDefaultHost(endpoint.host).setDefaultPort(endpoint.port)
      if (endpoint.tls) {
        httpOptions.setSsl(true).isUseAlpn = true
      } else {
        // Plaintext gRPC uses HTTP/2 with prior knowledge (no h2c upgrade dance).
        httpOptions.setHttp2ClearTextUpgrade(false)
      }
      return GrpcClient.client(vertx, httpOptions)
    }

    private fun Error.toStreamException(): IngestionStreamException =
        IngestionStreamException(kind.toIngestionErrorKind(), message, null)

    private fun ErrorKind.toIngestionErrorKind(): IngestionStreamException.Kind =
        when (this) {
          ErrorKind.ERROR_KIND_SHUTTING_DOWN -> IngestionStreamException.Kind.SHUTTING_DOWN
          ErrorKind.ERROR_KIND_UNKNOWN_SERVICE -> IngestionStreamException.Kind.UNKNOWN_SERVICE
          ErrorKind.ERROR_KIND_UNKNOWN_HANDLER -> IngestionStreamException.Kind.UNKNOWN_HANDLER
          else -> IngestionStreamException.Kind.UNKNOWN
        }
  }
}
