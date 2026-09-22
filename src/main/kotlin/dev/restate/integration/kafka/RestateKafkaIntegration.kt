package dev.restate.integration.kafka

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.vertx.core.Deployable
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerOptions
import io.vertx.launcher.application.HookContext
import io.vertx.launcher.application.VertxApplication
import io.vertx.launcher.application.VertxApplicationHooks
import io.vertx.micrometer.MicrometerMetricsOptions
import io.vertx.micrometer.VertxPrometheusOptions
import io.vertx.micrometer.backends.BackendRegistries
import java.util.function.Supplier
import org.apache.logging.log4j.LogManager

/**
 * Runs the Kafka -> Restate ingestion.
 *
 * This class is meant to be used in the main class of a uber-jar.
 */
class RestateKafkaIntegration private constructor(private val config: RestateKafkaIntegrationConfiguration) {

  private val hooks = ConsumerApplication(config)

  /**
   * Start the integration.
   * Returns the launcher exit code (0 = deployed successfully);
   * a startup failure terminates the JVM, exactly as the standalone launcher does.
   */
  fun run(): Int {
    // Route Vert.x's (and the launcher's own) logging to Log4j2 so startup/deploy errors are
    // visible. Must be set before the launcher creates Vert.x.
    System.setProperty(
        "vertx.logger-delegate-factory-class-name",
        "io.vertx.core.logging.Log4j2LogDelegateFactory",
    )
    return VertxApplication(emptyArray(), hooks).launch()
  }

  /** Stop the integration. No-op if it is not running. */
  fun stop() {
    val vertx = hooks.vertx ?: return
    vertx.close().toCompletionStage().toCompletableFuture().get()
  }

  companion object {
    /** Build loaded [RestateKafkaIntegrationConfiguration]. */
    fun fromConfig(config: RestateKafkaIntegrationConfiguration): RestateKafkaIntegration = RestateKafkaIntegration(config)
  }

  private class ConsumerApplication(private val config: RestateKafkaIntegrationConfiguration) : VertxApplicationHooks {

    private val log = LogManager.getLogger(RestateKafkaIntegration::class.java)

    // The Vert.x the launcher started, captured for stop(). Non-null once afterVertxStarted ran.
    var vertx: Vertx? = null
      private set

    // App metrics, wired to Vert.x's own Micrometer/Prometheus registry once Vert.x has started
    // (see
    // afterVertxStarted). Null when metrics are disabled. Read by verticleSupplier, which the
    // launcher invokes during deployment — after afterVertxStarted — so it always sees the resolved
    // value.
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
      vertx = context.vertx()
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
}
