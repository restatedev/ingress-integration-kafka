package dev.restate.integration.kafka

import io.vertx.core.Deployable
import io.vertx.launcher.application.HookContext
import io.vertx.launcher.application.VertxApplication
import io.vertx.launcher.application.VertxApplicationHooks
import java.util.function.Supplier
import kotlin.system.exitProcess
import org.apache.logging.log4j.LogManager

private val log = LogManager.getLogger("dev.restate.integration.kafka.Main")

fun main() {
  // Route Vert.x's (and the launcher's own) logging to Log4j2 so startup/deploy errors are visible.
  System.setProperty(
      "vertx.logger-delegate-factory-class-name",
      "io.vertx.core.logging.Log4j2LogDelegateFactory",
  )

  val config =
      try {
        AppConfig.load()
      } catch (e: Throwable) {
        log.error("Could not start the Kafka ingress integration, configuration error.", e)
        exitProcess(2)
      }

  log.info(
      "starting {} consumer instance(s) -> topics {} via {} at {}://{}:{}",
      config.restate.consumerInstances,
      config.restate.topics,
      config.recordMapper.javaClass.simpleName,
      if (config.restate.ingress.tls) "https" else "http",
      config.restate.ingress.host,
      config.restate.ingress.port,
  )

  VertxApplication(emptyArray(), ConsumerApplication(config)).launch()
}

private class ConsumerApplication(private val config: AppConfig) : VertxApplicationHooks {

  override fun beforeDeployingVerticle(context: HookContext) {
    // Deploy N consumer instances; Vert.x spreads them across event loops and Kafka spreads the
    // partitions across the instances.
    context.deploymentOptions().instances = config.restate.consumerInstances
  }

  override fun verticleSupplier(): Supplier<out Deployable> = Supplier { ConsumerVerticle(config) }

  override fun afterFailureToStartVertx(context: HookContext, t: Throwable) {
    log.error("failed to start Vertx", t)
  }

  override fun afterFailureToDeployVerticle(context: HookContext, t: Throwable) {
    log.error("failed to deploy consumer verticle", t)
  }
}
