package dev.restate.integration.client

import dev.restate.ingestion.v1.Invocation
import dev.restate.ingestion.v1.Request
import dev.restate.ingestion.v1.Response
import io.vertx.grpc.client.GrpcClientRequest

internal class InvocationStreamImpl(
    private val request: GrpcClientRequest<Request, Response>,
    private val listener: InvocationStream.Listener,
) : InvocationStream {

  // Remaining Restate send window, in BYTES, folded into isWritable() so the window never leaks to
  // callers. It may go negative: the protocol allows sending one invocation that overshoots the
  // window, after which isWritable() stays false until the next WindowUpdate replenishes it.
  private var budget = 0L

  init {
    // Transport-drain axis: a drained socket makes us writable again, if we still have budget.
    request.drainHandler { if (isWritable()) listener.onWritable(budget) }
  }

  /**
   * Apply a server window grant of [incrementBytes] (a 0 increment is a pure ack, grants nothing).
   */
  fun grantWindow(incrementBytes: Long) {
    val wasWritable = isWritable()
    budget += incrementBytes
    if (!wasWritable && isWritable()) listener.onWritable(budget)
  }

  override fun updateSettings(settings: StreamSettings) {
    request.write(Request.newBuilder().setSettings(settings).build())
  }

  override fun write(invocation: Invocation) {
    check(isWritable()) { "Stream is not writeable" }
    // Byte-based flow control: debit the serialized invocation size from the window.
    budget -= invocation.serializedSize.toLong()
    request.write(Request.newBuilder().setInvocation(invocation).build())
  }

  override fun isWritable(): Boolean = budget > 0 && !request.writeQueueFull()

  override fun end() {
    request.end()
    // Poison the budget
    budget = Long.MIN_VALUE
  }
}
