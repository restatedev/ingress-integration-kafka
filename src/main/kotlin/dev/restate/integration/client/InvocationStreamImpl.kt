package dev.restate.integration.client

import dev.restate.ingestion.v1.IngestionInvocation
import dev.restate.ingestion.v1.IngestionRequest
import dev.restate.ingestion.v1.IngestionResponse
import io.vertx.grpc.client.GrpcClientRequest

internal class InvocationStreamImpl(
    private val request: GrpcClientRequest<IngestionRequest, IngestionResponse>,
    private val listener: InvocationStream.Listener,
) : InvocationStream {

  // Remaining Restate send window, in BYTES, folded into isWritable() so the window never leaks to
  // callers. It may go negative: the protocol allows sending one invocation that overshoots the
  // window, after which isWritable() stays false until the next WindowUpdate replenishes it.
  //
  // Seeded with the protocol's guaranteed minimum window (INITIAL_WINDOW_BYTES): a freshly opened
  // stream may send up to that many bytes before the first WindowUpdate, so we don't wait for — and
  // the server no longer sends — an initial window grant. Later WindowUpdates just increment from
  // here, which is how the server grows the window up to its configured size (never below the min).
  private var budget = INITIAL_WINDOW_BYTES

  init {
    // Transport-drain axis: a drained socket makes us writable again, if we still have budget.
    request.drainHandler { if (isWritable()) listener.onWritable(budget) }
  }

  /**
   * Apply a server window grant of [incrementBytes] (a 0 increment is a pure ack, grants nothing).
   */
  fun grantWindow(incrementBytes: Int) {
    val wasWritable = isWritable()
    budget += incrementBytes
    if (!wasWritable && isWritable()) listener.onWritable(budget)
  }

  override fun updateDefaults(defaults: StreamDefaults) {
    request.write(IngestionRequest.newBuilder().setDefaults(defaults).build())
  }

  override fun write(invocation: IngestionInvocation) {
    check(isWritable()) { "Stream is not writeable" }
    // Byte-based flow control: debit the serialized invocation size from the window.
    budget -= invocation.serializedSize.toLong()
    request.write(IngestionRequest.newBuilder().setInvocation(invocation).build())
  }

  override fun isWritable(): Boolean = budget > 0 && !request.writeQueueFull()

  override fun end() {
    request.end()
    // Poison the budget
    budget = Long.MIN_VALUE
  }

  companion object {
    // The ingestion protocol guarantees a minimum initial send window of 32 KiB. The server grows
    // the window above this via WindowUpdate frames, but never shrinks it below this minimum.
    const val INITIAL_WINDOW_BYTES = 32L * 1024
  }
}
