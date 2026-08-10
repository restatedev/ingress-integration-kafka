package dev.restate.integration.client

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpVersion
import io.vertx.core.net.Address
import io.vertx.grpc.client.GrpcClient
import io.vertx.grpc.client.GrpcClientRequest
import io.vertx.grpc.common.ServiceMethod

internal class MultiEndpointGrpcClient(
    vertx: Vertx,
    endpoints: List<IngressEndpoint>,
) : GrpcClient {

  private val clients: List<GrpcClient> = endpoints.map { buildGrpcClient(vertx, it) }

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

  private fun selectGrpcClient(): GrpcClient {
    return clients.random()
  }

  override fun request(server: Address?): Future<GrpcClientRequest<Buffer?, Buffer?>?>? =
      selectGrpcClient().request(server)

  override fun request(): Future<GrpcClientRequest<Buffer?, Buffer?>?>? =
      selectGrpcClient().request()

  override fun <Req, Resp> request(
      server: Address?,
      method: ServiceMethod<Resp?, Req?>?,
  ): Future<GrpcClientRequest<Req?, Resp?>?>? = selectGrpcClient().request(server, method)

  override fun <Req, Resp> request(
      method: ServiceMethod<Resp?, Req?>?
  ): Future<GrpcClientRequest<Req?, Resp?>?>? = selectGrpcClient().request(method)

  override fun close(): Future<Void?>? = Future.all(clients.map { it.close() }).mapEmpty<Void>()
}
