package dev.restate.integration.kafka

import dev.restate.integration.kafka.config.AppConfig
import io.vertx.core.Deployable
import io.vertx.launcher.application.HookContext
import io.vertx.launcher.application.VertxApplication
import io.vertx.launcher.application.VertxApplicationHooks
import java.util.function.Supplier
import kotlin.system.exitProcess
import org.apache.logging.log4j.LogManager

/**
 * Entrypoint. Loads/validates configuration, then hands off to Vert.x's [VertxApplication] launcher
 * which owns Vertx creation, deployment, and graceful shutdown on SIGTERM/SIGINT (close Vertx ->
 * verticle `stop()` flushes commits and closes streams). We only customize it via hooks to deploy
 * the requested number of [ConsumerVerticle] instances. Vert.x itself (event-loop pool size, etc.)
 * is tunable through the launcher's `VERTX_*` environment variables.
 */
private val log = LogManager.getLogger("dev.restate.integration.kafka.Main")

fun main() {
  // Route Vert.x's (and the launcher's own) logging to Log4j2 so startup/deploy errors are visible.
  System.setProperty(
      "vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.Log4j2LogDelegateFactory")

  val config =
      try {
        AppConfig.load()
      } catch (e: Throwable) {
        log.error("Could not start the Kafka ingress integration, configuration error.", e)
        exitProcess(2)
      }

  log.info(
      "starting {} consumer instance(s) -> {} target {}/{} at {}://{}:{}",
      config.consumerInstances,
      config.topics,
      config.targetService,
      config.targetHandler,
      if (config.ingress.tls) "https" else "http",
      config.ingress.host,
      config.ingress.port)

  // launch() deploys and returns; the JVM stays alive on Vert.x's threads and the launcher's own
  // shutdown hook closes Vertx gracefully on SIGTERM/SIGINT. On a startup failure the launcher
  // exits the process itself, so we must NOT wrap this in exitProcess.
  VertxApplication(emptyArray(), ConsumerApplication(config)).launch()
}

private class ConsumerApplication(private val config: AppConfig) : VertxApplicationHooks {

  override fun beforeDeployingVerticle(context: HookContext) {
    // Deploy N consumer instances; Vert.x spreads them across event loops and Kafka spreads the
    // partitions across the instances.
    context.deploymentOptions().instances = config.consumerInstances
  }

  override fun verticleSupplier(): Supplier<out Deployable> = Supplier {
    ConsumerVerticle(config)
  }

  override fun afterFailureToStartVertx(context: HookContext, t: Throwable) {
    log.error("failed to start Vertx", t)
  }

  override fun afterFailureToDeployVerticle(context: HookContext, t: Throwable) {
    log.error("failed to deploy consumer verticle", t)
  }
}
