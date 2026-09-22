package dev.restate.integration.kafka.ingestion

import dev.restate.integration.ExactlyOnceProducer
import dev.restate.integration.ProducerOptions

/** Opens a fresh producer. */
@Suppress("UnstableApiUsage")
fun interface ProducerFactory {
  /**
   * Open a new [dev.restate.integration.ExactlyOnceProducer] for [producerId].
   * Called once per (re)connection by [PartitionSession].
   */
  fun newExactlyOnceProducer(producerId: String, options: ProducerOptions): ExactlyOnceProducer
}