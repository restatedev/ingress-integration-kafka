package dev.restate.integration.kafka

import dev.restate.ingestion.v1.ErrorKind
import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Settings
import io.vertx.core.Vertx
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import kotlin.system.exitProcess

/** The Kafka-side controls a [PartitionStream] needs; implemented by the owning consumer verticle. */
interface PartitionControl {
  /** Stop fetching this partition. */
  fun pause()

  /** Resume fetching this partition. */
  fun resume()

  /** Commit [offset] (the next offset to consume, i.e. lastCommitted + 1) for this partition. */
  fun commit(offset: Long)

  /** Reposition the consumer to [offset] so uncommitted records are re-read after a reconnect. */
  fun seek(offset: Long)
}

/**
 * One `Ingest` gRPC stream for a single (topic, partition): owns the stream lifecycle, drives the
 * [PartitionFlow] state machine, commits offsets on Restate's confirmation, and reconnects with
 * backoff on transient failures. A fatal (misconfiguration) error escalates via [onFatal].
 *
 * Lives entirely on its owning verticle's event loop, so its mutable state needs no locking.
 */
class PartitionStream(
    private val topic: String,
    private val partition: Int,
    private val settings: Settings,
    private val client: IngestionClient,
    private val control: PartitionControl,
    private val vertx: Vertx,
    private val scope: CoroutineScope,
) : IngestionStreamListener {

  private val log = LogManager.getLogger(PartitionStream::class.java)
  private val id = "$topic-$partition"

  private var stream: IngestionStream? = null
  private var flow: PartitionFlow? = null
  private var closing = false
  private var reconnecting = false
  private var reconnectAttempts = 0

  /**
   * Earliest offset we've sent that Restate hasn't confirmed durable yet (null if nothing sent).
   * On reconnect we re-seek here so no sent-but-unconfirmed record is skipped; dedup on
   * `(producer_id, offset)` makes the re-sends idempotent.
   */
  private var firstUnconfirmedOffset: Long? = null

  /** Open the first stream for this partition. */
  suspend fun start() {
    open()
  }

  private suspend fun open() {
    if (closing) return
    // Start paused so nothing flows before the first window; the flow assumes a paused start.
    control.pause()
    val s = client.open(this)
    if (closing) {
      s.end()
      return
    }
    val f =
        PartitionFlow(
            send = { record ->
              if (firstUnconfirmedOffset == null) firstUnconfirmedOffset = record.offset
              s.sendRecord(record)
            },
            pause = { control.pause() },
            resume = { control.resume() },
            writeQueueFull = { s.writeQueueFull() },
        )
    s.drainHandler { if (!closing) f.onDrain() }
    stream = s
    flow = f
    reconnectAttempts = 0
    s.sendSettings(settings)
    log.debug("opened ingestion stream for {} (producer_id={})", id, settings.producerId)
  }

  /** A record arrived from Kafka for this partition. */
  fun offer(record: Record) {
    if (!closing) flow?.offer(record)
  }

  override fun onWindowUpdate(increment: Long) {
    if (!closing) flow?.addCredits(increment)
  }

  override fun onCommit(lastCommitted: Long) {
    if (closing) return
    // Everything <= lastCommitted is durable; the next unconfirmed offset is lastCommitted + 1.
    firstUnconfirmedOffset = lastCommitted + 1
    control.commit(lastCommitted + 1)
  }

  override fun onError(error: dev.restate.ingestion.v1.Error) {
    when (error.kind) {
      ErrorKind.ERROR_KIND_UNKNOWN_SERVICE,
      ErrorKind.ERROR_KIND_UNKNOWN_HANDLER -> {
        log.error("Fatal error, Restate rejected the producer stream with error code {}, reason: {}", error.kind, error.message)
        exitProcess(1)
      }
      else -> {
        log.warn("retryable error on {}: {} - {}", id, error.kind, error.message)
        reconnect()
      }
    }
  }

  override fun onClose(cause: Throwable?) {
    if (closing) return
    if (cause != null) log.warn("ingestion stream {} failed, will reconnect", id, cause)
    else log.info("ingestion stream {} closed by server, will reconnect", id)
    reconnect()
  }

  /** Permanently close this stream (partition revoked or shutting down). */
  fun close() {
    closing = true
    stream?.end()
    stream = null
    flow = null
  }

  private fun reconnect() {
    if (closing || reconnecting) return
    reconnecting = true
    stream?.end()
    stream = null
    flow = null
    val attempt = ++reconnectAttempts
    // Capped exponential backoff: 200ms, 400ms, ... up to 30s.
    val backoff = minOf(30_000L, 200L * (1L shl minOf(attempt, 7)))
    log.info("reconnecting {} in {}ms (attempt {})", id, backoff, attempt)
    vertx.setTimer(backoff) {
      scope.launch {
        reconnecting = false
        if (closing) return@launch
        // Re-read from the earliest unconfirmed offset so nothing is lost; dedup on
        // (producer_id, offset) makes any re-sends of already-durable records safe.
        firstUnconfirmedOffset?.let { control.seek(it) }
        try {
          open()
        } catch (e: Exception) {
          log.error("failed to reopen stream {}", id, e)
          reconnect()
        }
      }
    }
  }
}
