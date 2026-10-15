// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka.ingestion

import dev.restate.integration.ExactlyOnceProducer
import dev.restate.integration.IntegrationClientException
import dev.restate.integration.Invocation
import dev.restate.integration.ProducerOptions
import dev.restate.integration.SendAttempt
import dev.restate.integration.SendResult
import dev.restate.integration.kafka.RetryPolicy
import io.vertx.core.Context
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager

/**
 * A resilient producer session for one logical producer (one Kafka topic-partition).
 *
 * The sdk [ExactlyOnceProducer] owns a single ingestion stream and terminates permanently on
 * failure; reconnection is the caller's job. This session supplies it: it opens a producer, feeds
 * it records with backpressure, forwards durable acks to the source, and — on a retryable failure —
 * rewinds the source to the first not-yet-committed offset and opens a fresh producer with the same
 * `producerId` (so the server's offset-based dedup drops anything already committed).
 *
 * The server tears the stream down on any error, including a record-scoped one that rejects a
 * single record (an [IntegrationClientException] carrying
 * [IntegrationClientException.getInvocationOffset]). The session handles that by dropping the
 * offending record and resending the rest — it never spends the retry budget on it, since the
 * stream itself was healthy. A stream-scoped error (no offset) is either reconnected with backoff
 * or, if non-retryable/exhausted, escalated to the [listener].
 *
 * It has no channel/mailbox: the per-record handlers ([Connection]) never suspend, so the single
 * event-loop thread already serializes them; only the connect/backoff orchestration ([run])
 * suspends. Producer acknowledgement futures may complete on a gRPC callback thread, so their
 * continuations are marshalled back onto the owning verticle's context via [onContext].
 *
 * MUST be driven by a scope pinned to a single event-loop context.
 */
class PartitionSession(
    private val factory: ProducerFactory,
    private val producerId: String,
    private val options: ProducerOptions,
    private val control: InboundStreamController,
    private val retryPolicy: RetryPolicy,
    private val listener: Listener? = null,
) {

  companion object {
    private val LOG = LogManager.getLogger(PartitionSession::class.java)
  }

  /** Notified once when the session is permanently closed (no further reconnects). */
  interface Listener {
    suspend fun onSessionClosed()
  }

  /**
   * The upstream record source a [PartitionSession] drives. Implemented by the source integration
   * (the Kafka consumer verticle); the session calls these to apply backpressure and to recover on
   * reconnect.
   */
  interface InboundStreamController {
    /** Stop fetching records. */
    fun pause()

    /** Resume fetching records. */
    fun resume()

    /**
     * Acknowledge durability up to (and including) [lastCommitted]; the source may commit onwards.
     */
    fun ack(lastCommitted: Long)

    /** Reposition the source to [offset] so uncommitted records are re-read after a reconnect. */
    fun rewindToOffset(offset: Long)
  }

  private var job: Job? = null
  private var currentConnection: Connection? = null
  // The event-loop context captured at start(); ack/ready futures are marshalled back onto it.
  private var vertxContext: Context? = null

  // Durable-progress watermark, tracked across reconnects (a fresh producer forgets it).
  // committedOffset = highest offset Restate has durably acked; resumeOffset = the offset to
  // re-read from on reconnect (the first not-yet-committed record). resumeOffset is normally
  // committedOffset + 1, but a skipped poison record moves it past the gap. skipOffsets holds
  // rejected offsets that must be dropped if they are re-read before the watermark passes them.
  private var committedOffset: Long? = null
  private var resumeOffset: Long? = null
  private val skipOffsets = HashSet<Long>()

  // ---- command surface (call from the event-loop context) ----

  /** Launch the session. Idempotent: a second call while running is ignored. */
  fun start(scope: CoroutineScope) {
    if (job != null) return
    vertxContext = Vertx.currentContext()
    job = scope.launch {
      try {
        run()
      } catch (e: CancellationException) {
        throw e // close() cancelled us; let it propagate after the finally
      } catch (e: Throwable) {
        LOG.error("producer session {} crashed", producerId, e)
      } finally {
        currentConnection?.closeProducer()
        listener?.onSessionClosed()
      }
    }
  }

  /** A record arrived from the source. */
  fun offer(offset: Long, invocation: Invocation) {
    if (offset in skipOffsets) {
      // A previously-rejected record re-read during a resend: drop it instead of resending.
      LOG.warn("dropping previously-rejected record {}@{}", producerId, offset)
      return
    }
    // The first record ever offered fixes where a reconnect with no ack yet must rewind to.
    if (resumeOffset == null) resumeOffset = offset
    currentConnection?.offer(offset, invocation)
  }

  /** Permanently close the session (source revoked / shutdown). */
  suspend fun close() {
    job?.cancelAndJoin()
  }

  // ---- orchestration: open -> await failure -> back off & retry ----

  private suspend fun run() {
    // Keep the source paused while we open the producer; the connection resumes it once it is
    // writable (the producer opens on the protocol's guaranteed initial window).
    control.pause()

    // A fresh schedule; reset only once an open succeeds, so a run of failed opens still gives up.
    var backoff = retryPolicy.iterator()

    while (true) {
      val producer =
          try {
            factory.newExactlyOnceProducer(producerId, options)
          } catch (e: CancellationException) {
            throw e
          } catch (e: Exception) {
            // A synchronous open failure is treated like a retryable stream drop; the backoff is
            // NOT reset (that only happens once an open succeeds).
            if (!backoffAndContinue(toCause(e), backoff)) return
            continue
          }

      val connection = Connection(producer)
      currentConnection = connection

      // Open succeeded: reset retries, then start pumping (resumes the source).
      backoff = retryPolicy.iterator()
      connection.attach()

      // Suspend until the connection fails (a clean close only happens via close(), which cancels
      // this coroutine).
      val cause = connection.closed.await()
      connection.closeProducer()

      if (cause == null) return // clean close -> done (teardown in the launch's finally)

      val rejectedOffset = cause.invocationOffset
      if (rejectedOffset != null) {
        // Record-scoped error: this one record was rejected while the stream stayed well-formed.
        // Drop it and reconnect immediately — the connection is healthy, so this is progress, not
        // a failure, and must not spend the retry budget. Reopening resets the backoff anyway.
        skipRejected(rejectedOffset)
        prepareReconnect()
        continue
      }

      // Stream-scoped error: reconnect with backoff, or give up.
      if (!backoffAndContinue(cause, backoff)) return
    }
  }

  /**
   * React to the record at [offset] being rejected by the server. The record is dropped for good;
   * the rest of the stream is resent on reconnect. If nothing before it is still outstanding, we
   * simply advance the resume point past it; otherwise we resend from the current resume point and
   * drop this offset when it is re-read (see [offer]).
   */
  private fun skipRejected(offset: Long) {
    LOG.warn("skipping rejected record {}@{}", producerId, offset)
    if (offset == resumeOffset) {
      resumeOffset = offset + 1
    } else {
      skipOffsets.add(offset)
    }
  }

  /** Pause the source and rewind it to the first not-yet-committed offset (if any). */
  private fun prepareReconnect() {
    control.pause()
    resumeOffset?.let { control.rewindToOffset(it) }
  }

  /**
   * Decide whether to reconnect after a stream-scoped [cause]: `false` = give up (non-retryable or
   * the policy is exhausted). On `true`, the source has been paused, rewound to the resume point,
   * and the backoff waited out.
   */
  private suspend fun backoffAndContinue(
      cause: IntegrationClientException,
      backoff: RetryPolicy.Iterator,
  ): Boolean {
    if (!cause.isRetryable()) {
      LOG.warn("permanent ingestion error for {}", producerId, cause)
      return false
    }
    val nextDelay = backoff.next()
    if (nextDelay == null) {
      LOG.warn("giving up on {} after {} attempts", producerId, backoff.attempts, cause)
      return false
    }
    prepareReconnect()
    LOG.info("reconnecting {} in {}", producerId, nextDelay, cause)
    delay(nextDelay)
    return true
  }

  private fun onContext(block: () -> Unit) {
    val ctx = vertxContext
    if (ctx == null || Vertx.currentContext() === ctx) block() else ctx.runOnContext { block() }
  }

  private fun toCause(err: Throwable): IntegrationClientException {
    val unwrapped = if (err is CompletionException && err.cause != null) err.cause!! else err
    // trySend on a terminally-closed producer throws IllegalStateException wrapping the real cause.
    val effective = (unwrapped as? IllegalStateException)?.cause ?: unwrapped
    return effective as? IntegrationClientException
        ?: IntegrationClientException(
            IntegrationClientException.Kind.UNKNOWN,
            effective.message ?: effective.toString(),
            err,
        )
  }

  // Only ever asked of a stream-scoped error (record-scoped ones are recovered by skipping,
  // whatever their kind). A stream-scoped BAD_REQUEST means a bad Start/IngestionDefaults, which
  // will recur on every reconnect, so it is fatal; every other kind is worth retrying.
  private fun IntegrationClientException.isRetryable(): Boolean =
      kind != IntegrationClientException.Kind.BAD_REQUEST

  /**
   * One live connection == one producer. Owns its send-side buffer and its terminal state. A
   * superseded connection is inert (the `currentConnection` guard), so a dropped producer's late
   * callbacks can't disturb the live one.
   */
  private inner class Connection(private val producer: ExactlyOnceProducer) {

    // Completes (with the cause) when this connection fails; null is reserved for a clean close.
    val closed = CompletableDeferred<IntegrationClientException?>()

    // Records accepted from the source but not yet handed to the producer (a backlog while
    // backpressured). paused = whether the source is currently paused.
    private val buffer = ArrayDeque<Pair<Long, Invocation>>()
    private var paused = true

    // Reentrancy guard for pump(): an inline future completion must not recurse into the loop.
    private var pumping = false
    private var pumpPending = false

    /** Start pumping; the producer opens writable, so this resumes the source right away. */
    fun attach() {
      pump()
    }

    fun offer(offset: Long, invocation: Invocation) {
      if (closed.isCompleted) return
      buffer.addLast(offset to invocation)
      pump()
    }

    fun closeProducer() {
      try {
        producer.close()
      } catch (e: Exception) {
        LOG.debug("error closing producer for {}", producerId, e)
      }
    }

    private fun fail(cause: IntegrationClientException) {
      if (currentConnection !== this) return
      closed.complete(cause)
    }

    /** Drive the send loop, coalescing any reentrant call triggered by an inline completion. */
    private fun pump() {
      if (pumping) {
        pumpPending = true
        return
      }
      pumping = true
      try {
        do {
          pumpPending = false
          pumpOnce()
        } while (pumpPending)
      } finally {
        pumping = false
      }
    }

    private fun pumpOnce() {
      val p = producer
      while (buffer.isNotEmpty() && !closed.isCompleted) {
        val (offset, invocation) = buffer.first()
        val attempt =
            try {
              p.trySend(offset, invocation)
            } catch (e: IllegalStateException) {
              // The producer is terminally closed.
              fail(toCause(e))
              return
            }
        when (attempt) {
          is SendAttempt.Accepted -> {
            buffer.removeFirst()
            attachAck(offset, attempt.acknowledgement())
          }
          is SendAttempt.Backpressured -> {
            // Leave the record buffered; pause the source and retry when the producer is ready.
            if (!paused) {
              control.pause()
              paused = true
            }
            armReady(attempt.ready())
            return
          }
        }
      }

      // Backlog drained and writable: resume the source.
      if (buffer.isEmpty() && !closed.isCompleted && paused) {
        control.resume()
        paused = false
      }
    }

    private fun armReady(ready: CompletableFuture<Void?>) {
      ready.whenComplete { _, err ->
        onContext {
          if (currentConnection !== this) return@onContext
          if (err != null) fail(toCause(err)) else pump()
        }
      }
    }

    private fun attachAck(offset: Long, ack: CompletableFuture<SendResult>) {
      ack.whenComplete { _, err ->
        onContext {
          if (currentConnection !== this) return@onContext
          if (err != null) fail(toCause(err)) else onDurableAck(offset)
        }
      }
    }
  }

  /**
   * A record was durably committed by Restate. Advances the watermark (acks are cumulative and
   * arrive in ascending offset order), forwards it to the source, and drops any skipped offsets the
   * watermark has now moved past.
   */
  private fun onDurableAck(offset: Long) {
    val prev = committedOffset
    if (prev != null && offset <= prev) return
    committedOffset = offset
    resumeOffset = offset + 1
    if (skipOffsets.isNotEmpty()) skipOffsets.removeIf { it <= offset }
    control.ack(offset)
  }
}
