package dev.restate.integration.client

import dev.restate.ingestion.v1.Request
import dev.restate.ingestion.v1.Response
import io.vertx.grpc.client.GrpcClientRequest

internal class IngestionStreamImpl(
    private val request: GrpcClientRequest<Request, Response>,
    private val producerId: String,
    private val listener: IngestionStream.Listener,
) : IngestionStream {

  // Remaining Restate window credits, folded into isWritable() so the window never leaks to
  // callers.
  private var budget = 0L

  init {
    // Transport-drain axis: a drained socket makes us writable again, if we still have budget.
    request.drainHandler { if (isWritable()) listener.onWritable(budget) }
  }

  /** Apply a server window grant (an [increment] of 0 is a pure ack and grants nothing). */
  fun grantWindow(increment: Long) {
    val wasWritable = isWritable()
    budget += increment
    if (!wasWritable && isWritable()) listener.onWritable(budget)
  }

  override fun updateSettings(settings: StreamSettings) {
    request.write(
        Request.newBuilder()
            .setSettings(settings.toBuilder().setProducerId(producerId).build())
            .build()
    )
  }

  override fun write(invocation: Invocation) {
    check(isWritable()) {
      "Stream is not writeable"
    }
    budget--
    request.write(Request.newBuilder().setRecord(invocation).build())
  }

  override fun isWritable(): Boolean = budget > 0 && !request.writeQueueFull()

  override fun end() {
    request.end()
  }
}
