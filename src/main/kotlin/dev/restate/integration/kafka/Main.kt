// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka

import kotlin.system.exitProcess
import org.apache.logging.log4j.LogManager

private val log = LogManager.getLogger("dev.restate.integration.kafka.Main")

fun main() {
  val config =
      try {
        RestateKafkaIntegrationConfiguration.load()
      } catch (e: Throwable) {
        log.error("Could not start the Kafka ingress integration, configuration error.", e)
        exitProcess(2)
      }

  log.info(
      "starting {} consumer instance(s) -> topics {} via {} at {}",
      config.restate.consumerInstances,
      config.restate.topics,
      config.recordMapper.javaClass.simpleName,
      config.restate.ingress,
  )

  RestateKafkaIntegration.fromConfig(config).run()
}
