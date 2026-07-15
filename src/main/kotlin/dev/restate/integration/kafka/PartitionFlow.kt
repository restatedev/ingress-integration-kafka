package dev.restate.integration.kafka

import dev.restate.ingestion.v1.Record

/**
 * The pull-based flow-control state machine for a single (topic, partition) stream.
 *
 * It is deliberately free of Vert.x, Kafka and gRPC types so the credit/buffer/pause logic can be
 * unit-tested in isolation. All methods must be called from a single thread (the owning verticle's
 * event loop), so no synchronization is needed.
 *
 * Model: Restate grants send credits via [addCredits]; records arriving from Kafka are handed to
 * [offer]. Records are only sent while credits remain and the sink is writable. When we run out of
 * credit (or the sink backs up) we ask Kafka to stop fetching via [pause]; when we regain credit and
 * have drained our backlog we [resume]. Because a Kafka poll batch can overrun the exact credit, any
 * surplus is buffered and flushed as new credit arrives — so "pull" is precise to a poll batch.
 *
 * @param send emit one record to the ingestion stream
 * @param pause ask Kafka to stop fetching this partition
 * @param resume ask Kafka to fetch this partition again
 * @param writeQueueFull whether the ingestion stream's write buffer is full (secondary backpressure)
 */
class PartitionFlow(
    private val send: (Record) -> Unit,
    private val pause: () -> Unit,
    private val resume: () -> Unit,
    private val writeQueueFull: () -> Boolean = { false },
) {
  private val buffer = ArrayDeque<Record>()

  /** Remaining records we're allowed to send before the next window update. */
  var credits: Long = 0L
    private set

  /** Highest offset handed to [send] so far, or null if nothing sent yet. */
  var lastSentOffset: Long? = null
    private set

  // We assume the partition starts paused (the owner pauses it before opening the stream), so we
  // don't emit a redundant pause() on construction.
  private var paused = true

  fun bufferedCount(): Int = buffer.size

  fun isPaused(): Boolean = paused

  /** A record arrived from Kafka. Buffer it and flush whatever the current window allows. */
  fun offer(record: Record) {
    buffer.addLast(record)
    deliver()
  }

  /** Restate granted [increment] more send credits (may be 0 as a pure ack). */
  fun addCredits(increment: Long) {
    credits += increment
    deliver()
  }

  /** The ingestion stream's write buffer drained; try to flush more. */
  fun onDrain() {
    deliver()
  }

  private fun deliver() {
    while (credits > 0 && buffer.isNotEmpty() && !writeQueueFull()) {
      val record = buffer.removeFirst()
      send(record)
      credits--
      lastSentOffset = record.offset
    }
    // Fetch more from Kafka only if we have spare credit, no backlog, and the sink is writable.
    val wantMore = credits > 0 && buffer.isEmpty() && !writeQueueFull()
    if (wantMore && paused) {
      paused = false
      resume()
    } else if (!wantMore && !paused) {
      paused = true
      pause()
    }
  }
}
