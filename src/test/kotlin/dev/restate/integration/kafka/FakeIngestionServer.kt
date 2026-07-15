package dev.restate.integration.kafka

import dev.restate.ingestion.v1.Error
import dev.restate.ingestion.v1.ErrorKind
import dev.restate.ingestion.v1.IngestionSvcGrpcService
import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Request
import dev.restate.ingestion.v1.Response
import dev.restate.ingestion.v1.Settings
import dev.restate.ingestion.v1.WindowUpdate
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.grpc.server.GrpcServer
import io.vertx.grpc.server.GrpcServerResponse
import io.vertx.kotlin.coroutines.coAwait
import java.util.concurrent.CopyOnWriteArrayList

/**
 * In-process fake of the Restate `IngestionSvc` for tests. Captures everything the client sends
 * and lets a test drive the server->client side (window grants, commits, errors) either
 * reactively (via [onSettings]/[onRecord] hooks that run on the server context) or imperatively
 * (via [grantWindow]/[commit]/[sendError], which hop onto the server context safely).
 */
class FakeIngestionServer(private val vertx: Vertx) {

  val settingsReceived = CopyOnWriteArrayList<Settings>()
  val recordsReceived = CopyOnWriteArrayList<Record>()

  /** Invoked on the server context when the client sends its Settings. */
  @Volatile var onSettings: ((Settings, GrpcServerResponse<Request, Response>) -> Unit)? = null

  /** Invoked on the server context for each Record the client sends. */
  @Volatile var onRecord: ((Record, GrpcServerResponse<Request, Response>) -> Unit)? = null

  @Volatile private var response: GrpcServerResponse<Request, Response>? = null
  @Volatile private var streamCtx: Context? = null
  private lateinit var httpServer: HttpServer

  var port: Int = -1
    private set

  suspend fun start(): FakeIngestionServer {
    val grpcServer = GrpcServer.server(vertx)
    grpcServer.callHandler(IngestionSvcGrpcService.Ingest) { req ->
      val resp = req.response()
      response = resp
      streamCtx = Vertx.currentContext()
      req.handler { request ->
        when {
          request.hasSettings() -> {
            settingsReceived.add(request.settings)
            onSettings?.invoke(request.settings, resp)
          }
          request.hasRecord() -> {
            recordsReceived.add(request.record)
            onRecord?.invoke(request.record, resp)
          }
        }
      }
      req.endHandler { resp.end() }
    }
    httpServer = vertx.createHttpServer().requestHandler(grpcServer)
    httpServer.listen(0).coAwait()
    port = httpServer.actualPort()
    return this
  }

  fun grantWindow(increment: Long) = onCtx {
    response?.write(
        Response.newBuilder()
            .setAck(WindowUpdate.newBuilder().setIncrement(increment))
            .build())
  }

  /** Send a commit watermark, optionally piggybacking a window increment. */
  fun commit(lastCommitted: Long, increment: Long = 0) = onCtx {
    response?.write(
        Response.newBuilder()
            .setLastCommitted(lastCommitted)
            .setAck(WindowUpdate.newBuilder().setIncrement(increment))
            .build())
  }

  fun sendError(kind: ErrorKind, message: String) = onCtx {
    response?.write(
        Response.newBuilder()
            .setError(Error.newBuilder().setKind(kind).setMessage(message))
            .build())
  }

  /** End the response stream from the server side. */
  fun endStream() = onCtx { response?.end() }

  suspend fun close() {
    httpServer.close().coAwait()
  }

  private fun onCtx(block: () -> Unit) {
    val ctx = streamCtx
    if (ctx != null) ctx.runOnContext { block() } else block()
  }
}
