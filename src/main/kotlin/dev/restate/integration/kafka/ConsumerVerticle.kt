package dev.restate.integration.kafka

import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Settings
import dev.restate.integration.kafka.config.AppConfig
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.HttpVersion
import io.vertx.grpc.client.GrpcClient
import io.vertx.kafka.client.common.TopicPartition
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.consumer.KafkaConsumerRecord
import io.vertx.kafka.client.consumer.OffsetAndMetadata
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.coAwait
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager

/**
 * One deployed instance == one `KafkaConsumer` (group member) on one event loop. Deploy N instances
 * (see [dev.restate.integration.kafka.config.AppConfig.consumerInstances]) to spread work across all
 * event loops; Kafka distributes partitions across the instances and rebalances automatically.
 *
 * All of an instance's state runs on its single context: the consumer, its assigned
 * [PartitionStream]s, their flow-control and gRPC streams — so no locking is required.
 */
class ConsumerVerticle(
    private val appConfig: AppConfig,
) : CoroutineVerticle() {

  private val log = LogManager.getLogger(ConsumerVerticle::class.java)

  private lateinit var ingestion: IngestionClient
  private lateinit var consumer: KafkaConsumer<String, ByteArray>
  private val partitions = HashMap<TopicPartition, PartitionStream>()

  override suspend fun start() {
    ingestion = IngestionClient(vertx, appConfig.ingress)
    consumer = KafkaConsumer.create(vertx, appConfig.kafkaConsumerConfig)

    consumer.handler { record ->
      val tp = TopicPartition(record.topic(), record.partition())
      val stream = partitions[tp]
      if (stream == null) {
        // Can happen briefly around a rebalance; the record will be redelivered after re-assignment.
        log.debug("dropping record for unassigned partition {}", tp)
      } else {
        stream.offer(toIngestionRecord(record))
      }
    }
    consumer.partitionsAssignedHandler { assigned -> assigned.forEach(::openPartition) }
    consumer.partitionsRevokedHandler { revoked -> revoked.forEach(::closePartition) }
    consumer.exceptionHandler { log.error("kafka consumer error", it) }

    consumer.subscribe(appConfig.topics.toSet()).coAwait()
    log.info("consumer instance subscribed to {} as group '{}'", appConfig.topics, appConfig.groupId)
  }

  override suspend fun stop() {
    log.info("stopping consumer instance ({} partitions)", partitions.size)
    partitions.values.forEach { it.close() }
    partitions.clear()
    if (::consumer.isInitialized) {consumer.close().coAwait() }
    if (::ingestion.isInitialized) {ingestion.close()}
  }

  private fun openPartition(tp: TopicPartition) {
    if (partitions.containsKey(tp)) return
    val settings =
        Settings.newBuilder()
            .setProducerId("${appConfig.groupId}/${tp.topic}/${tp.partition}")
            .setService(appConfig.targetService)
            .setHandler(appConfig.targetHandler)
            .build()
    val stream =
        PartitionStream(
            topic = tp.topic,
            partition = tp.partition,
            settings = settings,
            client = ingestion,
            control = controlFor(tp),
            vertx = vertx,
            scope = this,
        )
    partitions[tp] = stream
    launch {
      try {
        stream.start()
      } catch (e: Exception) {
        log.error("failed to open ingestion stream for {}", tp, e)
      }
    }
  }

  private fun closePartition(tp: TopicPartition) {
    partitions.remove(tp)?.close()
  }

  private fun controlFor(tp: TopicPartition) =
      object : PartitionControl {
        override fun pause() {
          consumer.pause(tp)
        }

        override fun resume() {
          consumer.resume(tp)
        }

        override fun commit(offset: Long) {
          consumer
              .commit(mapOf(tp to OffsetAndMetadata(offset, "")))
              .onFailure { log.warn("commit failed for {} @ {}", tp, offset, it) }
        }

        override fun seek(offset: Long) {
          consumer.seek(tp, offset).onFailure { log.warn("seek failed for {} @ {}", tp, offset, it) }
        }
      }

  private fun toIngestionRecord(record: KafkaConsumerRecord<String, ByteArray>): Record =
      RecordMapper.toRecord(
          topic = record.topic(),
          partition = record.partition(),
          offset = record.offset(),
          timestamp = record.timestamp(),
          key = record.key(),
          value = record.value(),
          headers = record.headers().map { it.key() to (it.value()?.bytes ?: ByteArray(0)) },
      )
}
