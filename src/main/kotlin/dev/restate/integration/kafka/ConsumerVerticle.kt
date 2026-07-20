package dev.restate.integration.kafka

import dev.restate.integration.client.InboundStreamController
import dev.restate.integration.client.IngestionClient
import dev.restate.integration.client.ProducerSession
import io.vertx.kafka.client.common.TopicPartition
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.consumer.OffsetAndMetadata
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.coAwait
import org.apache.logging.log4j.LogManager

/**
 * One deployed instance == one `KafkaConsumer` (group member) on one event loop. Deploy N instances
 * (see [RestateConfig.consumerInstances]) to spread work across all event loops; Kafka distributes
 * partitions across the instances and rebalances automatically.
 *
 * All of an instance's state runs on its single context: the consumer, its assigned
 * [ProducerSession]s and their gRPC streams — so no locking is required.
 */
class ConsumerVerticle(
    private val appConfig: AppConfig,
) : CoroutineVerticle() {

  private val log = LogManager.getLogger(ConsumerVerticle::class.java)

  private lateinit var ingestion: IngestionClient
  private lateinit var consumer: KafkaConsumer<Any?, Any?>
  private val partitions = HashMap<TopicPartition, ProducerSession>()

  override suspend fun start() {
    ingestion = IngestionClient.connect(vertx, appConfig.restate.ingress)
    consumer = KafkaConsumer.create<Any, Any>(vertx, appConfig.kafkaConsumerConfig)

    consumer.handler { record ->
      val tp = TopicPartition(record.topic(), record.partition())
      val session = partitions[tp]
      if (session == null) {
        // Can happen briefly around a rebalance; the record will be redelivered after
        // re-assignment.
        log.debug("dropping record for unassigned partition {}", tp)
      } else {
        session.offer(appConfig.recordMapper.toInvocation(record.record()))
      }
    }
    consumer.partitionsAssignedHandler { assigned -> assigned.forEach(::openPartition) }
    consumer.partitionsRevokedHandler { revoked -> revoked.forEach(::closePartition) }
    consumer.exceptionHandler { log.error("kafka consumer error", it) }

    consumer.subscribe(appConfig.restate.topics.toSet()).coAwait()
    log.info(
        "consumer instance subscribed to {} as group '{}'",
        appConfig.restate.topics,
        appConfig.groupId,
    )
  }

  override suspend fun stop() {
    log.info("stopping consumer instance ({} partitions)", partitions.size)
    val sessions = partitions.values.toList()
    partitions.clear()
    sessions.forEach { it.close() }
    if (::consumer.isInitialized) consumer.close().coAwait()
    if (::ingestion.isInitialized) ingestion.close()
  }

  private fun openPartition(tp: TopicPartition) {
    if (partitions.containsKey(tp)) return
    val session =
        ProducerSession(
            client = ingestion,
            id = "${appConfig.groupId}/${tp.topic}/${tp.partition}",
            control = controlFor(tp),
            retryPolicy = appConfig.restate.retryPolicy,
            listener =
                object : ProducerSession.Listener {
                  override fun onSessionClosed() {
                    log.info("ingestion session for {} closed", tp)
                    partitions.remove(tp)
                  }
                },
        )
    partitions[tp] = session
    session.start(this, appConfig.recordMapper.initialSettings())
  }

  private fun closePartition(tp: TopicPartition) {
    partitions.remove(tp)?.close()
  }

  private fun controlFor(tp: TopicPartition) =
      object : InboundStreamController {
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
