package dev.restate.integration.client

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager

/**
 * A resilient producer session for one logical producer (one `Ingest` stream at a time).
 *
 * Pulls records from an [InboundStreamController] and writes them into a Restate ingestion stream,
 * reconnecting with backoff on transient failures and re-reading from the last acked offset. It has
 * no channel/mailbox: the per-event handlers ([Connection]) never suspend, so the single event-loop
 * thread already serializes them; only the connect/backoff orchestration ([run]) suspends.
 *
 * Split of responsibilities:
 * - [Connection] owns one stream's send-side state and *is* its [IngestionStream.Listener]; a
 *   superseded connection is inert (the `currentConnection` guard), so a dropped stream's late
 *   events can't disturb the live one.
 * - [run] is a thin coroutine: `open` → `await` the close → back off & retry. It touches connection
 *   state only while running, and is parked exactly while the callbacks mutate it — no locking.
 *
 * Backoff is `delay(...)`; [close] is `job.cancel()`, so an in-flight open/await/delay tears down
 * at once. MUST be driven by a scope pinned to a single event-loop context.
 */
class ProducerSession(
    private val client: IngestionClient,
    private val id: String,
    private val control: InboundStreamController,
    private val retryPolicy: RetryPolicy,
    private val listener: Listener? = null,
) {

  /** Notified once when the session is permanently closed (no further reconnects). */
  interface Listener {
    fun onSessionClosed()
  }

  private var job: Job? = null
  private var currentConnection: Connection? = null

  // ---- command surface (call from the event-loop context) ----

  /** Launch the session. Idempotent: a second call while running is ignored. */
  fun start(scope: CoroutineScope, settings: StreamSettings) {
    if (job != null) return
    job = scope.launch {
      try {
        run(settings)
      } catch (e: CancellationException) {
        throw e // close() cancelled us; let it propagate after the finally
      } catch (e: Throwable) {
        LOG.error("producer session {} crashed", id, e)
      } finally {
        // Teardown current connection, notify listener.
        currentConnection?.end()
        listener?.onSessionClosed()
      }
    }
  }

  /** A record arrived from the source. */
  fun offer(invocation: Invocation) {
    currentConnection?.offer(invocation)
  }

  /** Permanently close the session (source revoked / shutdown). */
  fun close() {
    job?.cancel()
  }

  // ---- orchestration: open -> await close -> back off & retry ----

  private suspend fun run(settings: StreamSettings) {
    // No records until the server grants the first window.
    control.pause()

    var lastCommitted: Long? = null
    var backoff = retryPolicy.iterator()

    while (true) {
      currentConnection = Connection()

      try {
        val stream =
            try {
              client.open(id, currentConnection!!)
            } catch (e: CancellationException) {
              throw e
            } catch (e: Exception) {
              // Treat a failed open like a retryable stream drop (see the catch below).
              throw IngestionStreamException(IngestionStreamException.Kind.UNKNOWN, e.message, e)
            }
        currentConnection!!.attach(stream)
        stream.updateSettings(settings)

        // Connection established, reset retries.
        backoff = retryPolicy.iterator()

        // Wait for the connection to close.
        currentConnection!!.closed.await()

        return // clean close -> done (teardown in the launch's finally)
      } catch (e: IngestionStreamException) {
        // Carry the durable watermark forward before deciding, so any rewind uses the latest
        // offset.
        lastCommitted = currentConnection?.lastCommitted ?: lastCommitted

        // If it's not retryable, quit.
        if (!e.isRetryable()) {
          LOG.warn("permanent ingestion error for {}: {} - {}", id, e.kind, e.message)
          return
        }

        // Get the next backoff delay; null means the policy is exhausted.
        val nextDelay = backoff.next()
        if (nextDelay == null) {
          LOG.warn("giving up on {} after {} attempts", id, backoff.attempts)
          return
        }

        // Pause the source, rewind if we ever committed, then wait out the backoff.
        control.pause()
        lastCommitted?.let { control.rewindToOffset(it) }
        LOG.info("reconnecting {} in {}", id, nextDelay)
        delay(nextDelay)
      }
    }
  }

  /**
   * One live connection == one stream. Owns its send-side state and is the stream's [Listener]. The
   * handlers are synchronous (they never suspend), so the event loop serializes them for free.
   */
  private inner class Connection : IngestionStream.Listener {
    var lastCommitted: Long? = null
    // Completes on a clean close; completes exceptionally with the cause on an error close.
    val closed = CompletableDeferred<Unit>()

    private var stream: IngestionStream? = null
    // buffer = poll-batch overrun not yet sent; paused = whether the source is paused.
    // The credit window lives inside the stream now (folded into isWritable), so no budget here.
    private val buffer = ArrayDeque<Invocation>()
    private var paused = true

    /** Bind the live stream so the pump can write to it. */
    fun attach(stream: IngestionStream) {
      this.stream = stream
    }

    /** Half-close the stream (teardown). */
    fun end() {
      stream?.end()
    }

    fun offer(invocation: Invocation) {
      buffer.addLast(invocation)
      pump()
    }

    override fun ack(lastAcked: Long) {
      // Instance check: ignore a callback that belongs to a superseded connection.
      if (currentConnection !== this) return
      this.lastCommitted = lastAcked
      control.ack(lastAcked)
    }

    override fun onWritable() {
      if (currentConnection !== this) return
      pump()
    }

    override fun onClose(cause: IngestionStreamException?) {
      if (currentConnection !== this) return
      if (cause == null) closed.complete(Unit) else closed.completeExceptionally(cause)
    }

    /**
     * Send while the stream is writable, then pause/resume the source to match. "Writable" already
     * folds in both the Restate window and the transport buffer, so this is just the standard
     * write-stream consumer loop — driven by new records ([offer]) and by [onWritable].
     */
    private fun pump() {
      val s = stream ?: return

      // Write out everything we can.
      while (buffer.isNotEmpty() && s.isWritable()) {
        s.write(buffer.removeFirst())
      }

      // Resume the source only with a drained backlog and a writable stream; pause otherwise.
      val wantMore = buffer.isEmpty() && s.isWritable()
      if (wantMore && paused) {
        control.resume()
        paused = false
      } else if (!wantMore && !paused) {
        control.pause()
        paused = true
      }
    }
  }

  companion object {
    private val LOG = LogManager.getLogger(ProducerSession::class.java)
  }
}
