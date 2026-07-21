package dev.restate.integration.client

import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Settings

typealias StreamSettings = Settings

typealias Invocation = Record

/**
 * A single open `Ingest` bidi stream == one Restate producer == one (topic, partition).
 *
 * The send side is a credit-gated write stream: [isWritable]/[Listener.onWritable] express **both**
 * the Restate flow-control window and the transport buffer as one backpressure signal, so callers
 * just "write while writable" and never track the window themselves. Reconnects aren't its concern
 * — that's the session's.
 */
interface IngestionStream {

  /** (Re)send the stream [StreamSettings]. The producer id is stamped by the implementation. */
  fun updateSettings(settings: StreamSettings)

  /** Write a single [Invocation]. Only call while [isWritable]. */
  fun write(invocation: Invocation)

  /**
   * True when the stream can accept another record right now — there's an unused Restate window
   * credit and the transport buffer isn't full. When it flips to false, wait for
   * [Listener.onWritable].
   */
  fun isWritable(): Boolean

  /** Half-close the client side of the stream (we're done sending). */
  fun end()

  /** Events the stream pushes to its owner. Invoked on the stream's context. */
  interface Listener {
    /** Restate acknowledged durable commit up to (and including) offset [lastAcked]. */
    fun ack(lastAcked: Long)

    /**
     * The stream became writable again — the window was replenished or the transport drained —
     * after [isWritable] had gone false. Resume writing.
     */
    fun onWritable(currentBudget: Long)

    /** The response stream ended cleanly ([cause] null) or failed ([cause] non-null). */
    fun onClose(cause: IngestionStreamException?)
  }
}
