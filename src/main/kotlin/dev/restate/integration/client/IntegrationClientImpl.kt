package dev.restate.integration.client

import dev.restate.ingestion.v1.Error
import dev.restate.ingestion.v1.ErrorKind
import dev.restate.ingestion.v1.IngestionSvcGrpcClient
import dev.restate.ingestion.v1.Request
import dev.restate.ingestion.v1.Response
import dev.restate.ingestion.v1.Start
import dev.restate.integration.version.Version
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpVersion
import io.vertx.grpc.client.GrpcClient
import io.vertx.grpc.common.GrpcStatus
import io.vertx.kotlin.coroutines.coAwait

internal class IntegrationClientImpl(
    private val grpcClient: GrpcClient,
    endpoint: IngressEndpoint,
) : IntegrationClient {

  private val authToken = endpoint.authToken

  override suspend fun open(
      producerId: String,
      listener: InvocationStream.Listener,
      initialStreamSettings: StreamSettings,
  ): InvocationStream {
    val request = grpcClient.request(IngestionSvcGrpcClient.Ingest).coAwait()
    request.headers().set(ACCEPT_VERSION_HEADER, SUPPORTED_VERSION)
    if (authToken != null) {
      request.headers().set("Authorization", "Bearer $authToken")
    }

    val stream =
        InvocationStreamImpl(
            request,
            listener,
        )
    try {
      // The mandatory Start handshake frame: stamps the producer id + integration identity and
      // carries the initial stream defaults. Must be the first frame on the stream.
      request
          .write(
              Request.newBuilder()
                  .setStart(
                      Start.newBuilder()
                          .setProducerId(producerId)
                          .setIntegration(Version.INTEGRATION)
                          .setSettings(initialStreamSettings)
                  )
                  .build()
          )
          .coAwait()

      // Let's wait for the response before handing the opened stream back.
      val response = request.response().coAwait()

      // Version negotiation: if the server echoes a version we don't speak,
      // it's a permanent misconfiguration, no need to retry.
      val negotiated = response.headers().get(VERSION_HEADER)
      if (negotiated != null && negotiated != SUPPORTED_VERSION) {
        throw IntegrationClientException(
            IntegrationClientException.Kind.BAD_REQUEST,
            "server negotiated unsupported ingestion protocol version '$negotiated' " +
                "(this client supports '$SUPPORTED_VERSION')",
        )
      }

      // Setup all the handlers
      response.handler { dispatch(it, stream, listener) }
      response.errorHandler { err ->
        listener.onClose(
            IntegrationClientException(
                IntegrationClientException.Kind.UNKNOWN,
                "ingestion stream failed with gRPC error: $err",
            )
        )
      }
      response.endHandler {
        // A non-OK gRPC status on end is a failure (e.g. version negotiation rejected), not a
        // clean close: surface it so the session retries per policy.
        val status = response.status()
        if (status == null || status == GrpcStatus.OK) {
          listener.onClose(null)
        } else {
          listener.onClose(
              IntegrationClientException(
                  IntegrationClientException.Kind.UNKNOWN,
                  "ingestion stream closed with gRPC status $status: ${response.statusMessage()}",
              )
          )
        }
      }
      response.exceptionHandler {
        listener.onClose(
            IntegrationClientException(
                IntegrationClientException.Kind.UNKNOWN,
                "got exception from response",
                it,
            )
        )
      }
    } catch (err: IntegrationClientException) {
      listener.onClose(err)
    } catch (err: Throwable) {
      listener.onClose(
          IntegrationClientException(
              IntegrationClientException.Kind.UNKNOWN,
              "could not send the request",
              err,
          )
      )
    }

    return stream
  }

  override suspend fun close() {
    grpcClient.close().coAwait()
  }

  private fun dispatch(
      response: Response,
      stream: InvocationStreamImpl,
      listener: InvocationStream.Listener,
  ) {
    // A Response may carry an ack watermark alongside its window/error; surface the ack first.
    if (response.hasLastCommitted()) {
      listener.ack(response.lastCommitted)
    }
    when {
      // The window grant is flow control, not an application event: feed it to the stream's budget.
      response.hasAck() -> stream.grantWindow(response.ack.incrementBytes)
      response.hasError() -> listener.onClose(response.error.toStreamException())
    }
  }

  companion object {
    /** Ingestion protocol version this client speaks (see the proto's version-negotiation note). */
    private const val SUPPORTED_VERSION = "1"
    private const val ACCEPT_VERSION_HEADER = "x-restate-integration-accept-version"
    private const val VERSION_HEADER = "x-restate-integration-version"

    fun connect(
        vertx: Vertx,
        endpoint: IngressEndpoint,
    ): IntegrationClientImpl =
        IntegrationClientImpl(
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

    private fun Error.toStreamException(): IntegrationClientException {
      // Include the offending offset when the server attributes the error to a specific invocation.
      val detail = if (hasInvocationOffset()) "[offset=$invocationOffset] $message" else message
      return IntegrationClientException(kind.toIngestionErrorKind(), detail, null)
    }

    private fun ErrorKind.toIngestionErrorKind(): IntegrationClientException.Kind =
        when (this) {
          ErrorKind.ERROR_KIND_SHUTTING_DOWN -> IntegrationClientException.Kind.SHUTTING_DOWN
          ErrorKind.ERROR_KIND_GO_AWAY -> IntegrationClientException.Kind.GO_AWAY
          ErrorKind.ERROR_KIND_NOT_FOUND -> IntegrationClientException.Kind.NOT_FOUND
          ErrorKind.ERROR_KIND_BAD_REQUEST -> IntegrationClientException.Kind.BAD_REQUEST
          else -> IntegrationClientException.Kind.UNKNOWN
        }
  }
}
