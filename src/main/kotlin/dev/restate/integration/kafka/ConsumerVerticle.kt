// Copyright (c) 2026 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Kafka ingress integration,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/ingress-integration-kafka/blob/main/LICENSE
package dev.restate.integration.kafka

import dev.restate.integration.ProducerOptions
import dev.restate.integration.kafka.ingestion.PartitionSession
import dev.restate.integration.kafka.ingestion.VertxMultiEndpointProducerFactory
import dev.restate.integration.version.Version
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics
import io.vertx.kafka.client.common.KafkaClientOptions
import io.vertx.kafka.client.common.TopicPartition
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.consumer.OffsetAndMetadata
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.coAwait
import java.util.concurrent.Callable
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer as NativeKafkaConsumer
import org.apache.logging.log4j.LogManager

/**
 * One deployed instance == one `KafkaConsumer` (group member) on one event loop. Deploy N instances
 * (see [RestateConfig.consumerInstances]) to spread work across all event loops; Kafka distributes
 * partitions across the instances and rebalances automatically.
 *
 * All of an instance's state runs on its single context: the consumer, its assigned
 * [PartitionSession]s and their ingestion streams — so no locking is required.
 */
@Suppress("UnstableApiUsage")
class ConsumerVerticle(
    private val restateKafkaIntegrationConfiguration: RestateKafkaIntegrationConfiguration,
    private val registry: MeterRegistry? = null,
) : CoroutineVerticle() {

  private val log = LogManager.getLogger(ConsumerVerticle::class.java)

  private lateinit var vertxMultiEndpointProducerFactory: VertxMultiEndpointProducerFactory
  // Stream defaults (target service/handler/headers) are the same for every partition.
  private val producerOptions: ProducerOptions =
      ProducerOptions.builder()
          .defaultMetadata(restateKafkaIntegrationConfiguration.recordMapper.initialDefaults())
          .build()

  private lateinit var consumer: KafkaConsumer<Any?, Any?>
  private val partitions = HashMap<TopicPartition, PartitionSession>()
  // Native Kafka consumer metrics binder (kafka.consumer.*); closed with the consumer.
  private var kafkaClientMetrics: AutoCloseable? = null

  override suspend fun start() {
    vertxMultiEndpointProducerFactory =
        VertxMultiEndpointProducerFactory(
            vertx,
            restateKafkaIntegrationConfiguration.restate.ingress,
            integrationName = "kafka",
            integrationVersion = "${Version.VERSION}_${Version.GIT_HASH}",
        )

    // Constructing the native consumer can block, e.g. SASL/OAUTHBEARER (OIDC) fetches the initial
    // token from the identity provider (with retries) during login. Build it off the event loop.
    val config: Map<String, Any> = restateKafkaIntegrationConfiguration.kafkaConsumerConfig
    val nativeConsumer =
        vertx.executeBlocking(Callable { NativeKafkaConsumer<Any?, Any?>(config) }).coAwait()
    consumer =
        KafkaConsumer.create(vertx, nativeConsumer, KafkaClientOptions.fromMap(config, false))
    kafkaClientMetrics = registry?.let { r ->
      KafkaClientMetrics(consumer.unwrap()).also { it.bindTo(r) }
    }

    consumer.handler { record ->
      val tp = TopicPartition(record.topic(), record.partition())
      val session = partitions[tp]
      if (session == null) {
        // Can happen briefly around a rebalance; the record will be redelivered after
        // re-assignment.
        log.debug("dropping record for unassigned partition {}", tp)
      } else {
        // A null mapping means the mapper filtered the record out: skip it (don't send it on).
        val invocation =
            restateKafkaIntegrationConfiguration.recordMapper.toInvocation(record.record())
        if (invocation == null) {
          log.trace("record mapper filtered out record {}@{}", tp, record.offset())
        } else {
          session.offer(record.offset(), invocation)
        }
      }
    }
    consumer.partitionsAssignedHandler { assigned -> assigned.forEach(::openPartition) }
    consumer.partitionsRevokedHandler { revoked ->
      launch { revoked.forEach { closePartition(it) } }
    }
    consumer.exceptionHandler { log.error("kafka consumer error", it) }

    consumer.subscribe(restateKafkaIntegrationConfiguration.restate.topics.toSet()).coAwait()
    log.info(
        "consumer instance subscribed to {} as group '{}'",
        restateKafkaIntegrationConfiguration.restate.topics,
        restateKafkaIntegrationConfiguration.groupId,
    )
  }

  override suspend fun stop() {
    log.info("stopping consumer instance ({} partitions)", partitions.size)
    val sessions = partitions.values.toList()

    // Close all producer sessions
    sessions.forEach { it.close() }
    if (::vertxMultiEndpointProducerFactory.isInitialized) vertxMultiEndpointProducerFactory.close()

    // Close Kafka consumer
    kafkaClientMetrics?.close()
    if (::consumer.isInitialized) consumer.close().coAwait()
  }

  private fun openPartition(tp: TopicPartition) {
    if (partitions.containsKey(tp)) return
    val producerId = "${restateKafkaIntegrationConfiguration.groupId}/${tp.topic}/${tp.partition}"
    val session =
        PartitionSession(
            factory = vertxMultiEndpointProducerFactory,
            producerId = producerId,
            options = producerOptions,
            control = controlFor(tp),
            retryPolicy = restateKafkaIntegrationConfiguration.restate.retryPolicy,
            listener =
                object : PartitionSession.Listener {
                  override suspend fun onSessionClosed() {
                    log.info("ingestion session for {} closed", tp)
                    closePartition(tp)
                  }
                },
        )
    partitions[tp] = session
    session.start(this)
  }

  private suspend fun closePartition(tp: TopicPartition) {
    partitions.remove(tp)?.close()
  }

  private fun controlFor(tp: TopicPartition) =
      object : PartitionSession.InboundStreamController {
        override fun pause() {
          consumer.pause(tp)
        }

        override fun resume() {
          consumer.resume(tp)
        }

        override fun ack(lastCommitted: Long) {
          // The Kafka commit offset is the next offset to consume, i.e. lastCommitted + 1.
          val offset = lastCommitted + 1
          consumer.commit(mapOf(tp to OffsetAndMetadata(offset, ""))).onFailure {
            log.warn("commit failed for {} @ {}", tp, offset, it)
          }
        }

        override fun rewindToOffset(offset: Long) {
          consumer.seek(tp, offset).onFailure {
            log.warn("seek failed for {} @ {}", tp, offset, it)
          }
        }
      }
}
