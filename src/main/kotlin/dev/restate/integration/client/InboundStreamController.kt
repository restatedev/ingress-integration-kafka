package dev.restate.integration.client

/**
 * The upstream record source a [ProducerSession] drives. Implemented by the source integration
 * (e.g. the Kafka consumer verticle); the session calls these to apply backpressure and to recover
 * on reconnect.
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
