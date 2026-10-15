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
import dev.restate.integration.ProducerOptions

/** Opens a fresh producer. */
@Suppress("UnstableApiUsage")
fun interface ProducerFactory {
  /**
   * Open a new [dev.restate.integration.ExactlyOnceProducer] for [producerId]. Called once per
   * (re)connection by [PartitionSession].
   */
  fun newExactlyOnceProducer(producerId: String, options: ProducerOptions): ExactlyOnceProducer
}
