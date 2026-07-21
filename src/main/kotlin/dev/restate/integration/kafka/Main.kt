package dev.restate.integration.kafka

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.vertx.core.Deployable
import io.vertx.core.http.HttpServerOptions
import io.vertx.launcher.application.HookContext
import io.vertx.launcher.application.VertxApplication
import io.vertx.launcher.application.VertxApplicationHooks
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import io.vertx.micrometer.backends.BackendRegistries
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

  // App metrics, wired to Vert.x's own Micrometer/Prometheus registry once Vert.x has started (see
  // afterVertxStarted). Null when metrics are disabled. Read by verticleSupplier, which the
  // launcher
  // invokes during deployment — after afterVertxStarted — so it always sees the resolved value.
  private var metricsRegistry: MeterRegistry? = null

  override fun beforeStartingVertx(context: HookContext) {
    if (!config.restate.metricsEnabled) return
    context
        .vertxOptions()
        .setMetricsOptions(
            MicrometerMetricsOptions()
                .setEnabled(true)
                .setPrometheusOptions(
                    VertxPrometheusOptions()
                        .setEnabled(true)
                        .setStartEmbeddedServer(true)
                        .setEmbeddedServerOptions(
                            HttpServerOptions().setPort(config.restate.metricsPort)
                        )
                )
        )
  }

  override fun afterVertxStarted(context: HookContext) {
    if (!config.restate.metricsEnabled) return
    val registry = BackendRegistries.getDefaultNow()
    if (registry == null) {
      log.warn("metrics enabled but no Micrometer backend registry was set up; metrics disabled")
      return
    }
    JvmMemoryMetrics().bindTo(registry)
    JvmGcMetrics().bindTo(registry)
    JvmThreadMetrics().bindTo(registry)
    ClassLoaderMetrics().bindTo(registry)
    ProcessorMetrics().bindTo(registry)
    this.metricsRegistry = registry
    log.info("exposing Prometheus metrics on :{}/metrics", config.restate.metricsPort)
  }

  override fun beforeDeployingVerticle(context: HookContext) {
    // Deploy N consumer instances; Vert.x spreads them across event loops and Kafka spreads the
    // partitions across the instances.
    context.deploymentOptions().instances = config.restate.consumerInstances
  }

  override fun verticleSupplier(): Supplier<out Deployable> = Supplier {
    ConsumerVerticle(config, metricsRegistry)
  }

  override fun afterFailureToStartVertx(context: HookContext, t: Throwable) {
    log.error("failed to start Vertx", t)
  }

  override fun afterFailureToDeployVerticle(context: HookContext, t: Throwable) {
    log.error("failed to deploy consumer verticle", t)
  }
}
