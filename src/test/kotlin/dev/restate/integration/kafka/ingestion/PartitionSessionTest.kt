package dev.restate.integration.kafka.ingestion

import dev.restate.integration.ExactlyOnceProducer
import dev.restate.integration.IntegrationClientException
import dev.restate.integration.IntegrationClientException.Kind
import dev.restate.integration.Invocation
import dev.restate.integration.InvocationMetadata
import dev.restate.integration.ProducerOptions
import dev.restate.integration.SendAttempt
import dev.restate.integration.SendResult
import dev.restate.integration.kafka.RetryPolicy
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * State-machine tests for [PartitionSession] in isolation — no Vert.x, no gRPC. The session runs on
 * the [runTest] virtual-time dispatcher; the sdk [ExactlyOnceProducer] (an interface) and the
 * [PartitionSession.InboundStreamController] are faked. Backpressure, durable acks and stream
 * failures are simulated by driving the fake producer directly.
 *
 * We only use stable coroutine-test APIs: [runTest] + [TestScope] + [delay]. A `delay` in the test
 * body advances the shared virtual clock, which lets the session's launched coroutine make progress
 * (and skips its backoff `delay`s instantly).
 */
class PartitionSessionTest {

  // ---- start / connect ----

  @Test
  fun `opens a producer for the producer id and pauses the source`() = test {
    it.connect()

    assertThat(it.factory.producerIds).containsExactly(PRODUCER_ID)
    assertThat(it.control.pauseCount).isGreaterThanOrEqualTo(1)
  }

  @Test
  fun `an open producer resumes the source`() = test {
    it.connect()

    // The producer opens writable, so attaching it resumes the source right away.
    assertThat(it.control.resumeCount).isEqualTo(1)
  }

  @Test
  fun `start is idempotent while running`() = test {
    it.connect()
    it.session.start(it.scope)
    settle()

    assertThat(it.factory.createCount).isEqualTo(1)
  }

  // ---- flow control ----

  @Test
  fun `while writable, offered records are sent`() = test {
    it.connect()

    it.session.offer(0, record(0))
    it.session.offer(1, record(1))

    assertThat(it.producer.sentOffsets).containsExactly(0L, 1L)
  }

  @Test
  fun `records offered while backpressured are buffered and pause the source`() = test {
    it.connect()
    it.producer.writable = false

    it.session.offer(0, record(0))
    it.session.offer(1, record(1))

    assertThat(it.producer.sentOffsets).isEmpty()
    assertThat(it.control.pauseCount).isGreaterThanOrEqualTo(1)
  }

  @Test
  fun `becoming ready flushes the backlog and resumes the source`() = test {
    it.connect()
    it.producer.writable = false
    it.session.offer(0, record(0))
    it.session.offer(1, record(1))

    it.producer.writable = true
    it.producer.readyFuture!!.complete(null)

    assertThat(it.producer.sentOffsets).containsExactly(0L, 1L)
    assertThat(it.control.resumeCount).isGreaterThanOrEqualTo(2)
  }

  // ---- ack ----

  @Test
  fun `a durable ack forwards the offset to the source`() = test {
    it.connect()
    it.session.offer(7, record(7))

    it.producer.ack(7)

    assertThat(it.control.acked).containsExactly(7L)
  }

  // ---- teardown ----

  @Test
  fun `a clean close notifies the listener once`() = test {
    it.connect()

    it.session.close()
    settle()

    assertThat(it.listener.closedCount).isEqualTo(1)
  }

  @Test
  fun `records offered after close are not sent`() = test {
    it.connect()
    it.session.close()
    settle()

    it.session.offer(0, record(0))

    assertThat(it.producer.sentOffsets).isEmpty()
  }

  @Test
  fun `a stream-scoped BAD_REQUEST tears the session down without reconnecting`() = test {
    it.connect()
    it.session.offer(0, record(0))

    // No invocation offset => stream-scoped (e.g. a bad Start/IngestionDefaults): fatal.
    it.producer.failStream(IntegrationClientException(Kind.BAD_REQUEST, "bad defaults"))
    settle()

    assertThat(it.listener.closedCount).isEqualTo(1)
    assertThat(it.factory.createCount).isEqualTo(1)
  }

  // ---- reconnect ----

  @Test
  fun `a retryable error reconnects without tearing down`() =
      test(fastRetry()) {
        it.connect()
        it.session.offer(0, record(0))

        it.producer.failStream(IntegrationClientException(Kind.SHUTTING_DOWN, "x"))
        settle()

        assertThat(it.factory.createCount).isEqualTo(2)
        assertThat(it.listener.closedCount).isEqualTo(0)
      }

  @Test
  fun `a GO_AWAY error reconnects without tearing down`() =
      test(fastRetry()) {
        it.connect()
        it.session.offer(0, record(0))

        it.producer.failStream(IntegrationClientException(Kind.GO_AWAY, "x"))
        settle()

        assertThat(it.factory.createCount).isEqualTo(2)
        assertThat(it.listener.closedCount).isEqualTo(0)
      }

  @Test
  fun `a retryable error after an ack rewinds to the offset after the last ack`() =
      test(fastRetry()) {
        it.connect()
        it.session.offer(10, record(10))
        it.producer.ack(10)
        it.session.offer(11, record(11))

        it.producer.failStream(IntegrationClientException(Kind.SHUTTING_DOWN, "x"))
        settle()

        // Offset 10 is durable; resume from the first uncommitted record, 11.
        assertThat(it.control.rewinds).containsExactly(11L)
      }

  @Test
  fun `a retryable error without a prior ack rewinds to the first offered offset`() =
      test(fastRetry()) {
        it.connect()
        it.session.offer(5, record(5))
        it.session.offer(6, record(6))

        it.producer.failStream(IntegrationClientException(Kind.SHUTTING_DOWN, "x"))
        settle()

        // Nothing acked yet, so every offered record must be resent from the first one.
        assertThat(it.control.rewinds).containsExactly(5L)
      }

  @Test
  fun `retries stop and tear down when the policy allows no retries`() =
      test(RetryPolicy(maxAttempts = 1)) {
        it.connect()
        it.session.offer(0, record(0))

        it.producer.failStream(IntegrationClientException(Kind.SHUTTING_DOWN, "x"))
        settle()

        assertThat(it.listener.closedCount).isEqualTo(1)
        assertThat(it.factory.createCount).isEqualTo(1)
      }

  @Test
  fun `a failed open is retried and can then succeed`() =
      test(fastRetry()) {
        it.factory.failOpens = 1

        it.connect()
        settle()

        assertThat(it.factory.createCount).isEqualTo(2)
        assertThat(it.listener.closedCount).isEqualTo(0)
      }

  @Test
  fun `repeated open failures give up once the policy is exhausted`() =
      test(fastRetry(maxAttempts = 3)) {
        it.factory.failOpens = Int.MAX_VALUE // every open fails

        it.connect()
        settle()

        assertThat(it.factory.createCount).isEqualTo(3)
        assertThat(it.listener.closedCount).isEqualTo(1)
      }

  @Test
  fun `late callbacks from a superseded connection are ignored`() =
      test(fastRetry()) {
        it.connect()
        val stale = it.producer // producer #1
        it.session.offer(0, record(0))
        it.session.offer(1, record(1))

        // A record-scoped failure tears connection #1 down and triggers a reconnect.
        stale.failAck(0, IntegrationClientException(Kind.SHUTTING_DOWN, "x"))
        settle() // reconnects -> connection #2 is now current

        // A straggler ack from the superseded producer #1 must be ignored.
        stale.ack(1)

        assertThat(it.control.acked).doesNotContain(1L)
      }

  // ---- record-scoped errors (skip the offending record) ----

  @Test
  fun `a record-scoped error skips the record and reconnects without giving up`() =
      test(fastRetry()) {
        it.connect()
        it.session.offer(5, record(5))

        // A record-scoped error (carries an offset), even a BAD_REQUEST, is recoverable by
        // skipping.
        it.producer.failStream(IntegrationClientException(Kind.BAD_REQUEST, "missing key", 5L))
        settle()

        assertThat(it.factory.createCount).isEqualTo(2)
        assertThat(it.listener.closedCount).isEqualTo(0)
        // Nothing was acked before it, so the resume point simply steps over the poison record.
        assertThat(it.control.rewinds).containsExactly(6L)
      }

  @Test
  fun `a record-scoped error does not spend the retry budget`() =
      // A single retry attempt would be exhausted by a stream-scoped error, but a record-scoped one
      // is progress, not a failure, so the session keeps going.
      test(RetryPolicy(maxAttempts = 1)) {
        it.connect()
        it.session.offer(0, record(0))

        it.producer.failStream(IntegrationClientException(Kind.NOT_FOUND, "no handler", 0L))
        settle()

        assertThat(it.factory.createCount).isEqualTo(2)
        assertThat(it.listener.closedCount).isEqualTo(0)
      }

  @Test
  fun `a rejected record behind outstanding ones is dropped when re-read`() =
      test(fastRetry()) {
        it.connect()
        it.session.offer(5, record(5))
        it.producer.ack(5) // commit 5, so the resume point is 6
        it.session.offer(6, record(6))
        it.session.offer(7, record(7))

        // 7 is rejected while 6 is still outstanding: 6 must be resent, 7 dropped on re-read.
        it.producer.failStream(IntegrationClientException(Kind.BAD_REQUEST, "bad", 7L))
        settle()

        assertThat(it.control.rewinds).containsExactly(6L)

        // Producer #2 re-receives the replayed records; 7 is filtered out, not resent.
        it.session.offer(6, record(6))
        it.session.offer(7, record(7))
        it.session.offer(8, record(8))
        assertThat(it.producer.sentOffsets).containsExactly(6L, 8L)
      }

  // ---- harness ----

  /** Comfortably longer than any backoff a test uses; delays are virtual, so this is free. */
  private suspend fun settle() = delay(100.milliseconds)

  private fun test(retry: RetryPolicy = RetryPolicy(), body: suspend TestScope.(Env) -> Unit) =
      runTest {
        val env = Env(this, retry)
        try {
          body(env)
        } finally {
          env.session.close()
          settle()
        }
      }
}

private const val PRODUCER_ID = "group/topic/0"

private fun record(offset: Long): Invocation =
    Invocation.create()
        .setServiceName("Svc")
        .setHandlerName("h")
        .setBody("payload-$offset".toByteArray())

private fun fastRetry(maxAttempts: Int? = null) =
  RetryPolicy(initialInterval = 1.milliseconds, maxAttempts = maxAttempts)

private class Env(val scope: TestScope, retry: RetryPolicy) {
  val factory = FakeFactory()
  val control = FakeInboundStreamController()
  val listener = RecordingListener()
  private val options =
      ProducerOptions.builder()
          .defaultMetadata(InvocationMetadata.create().setServiceName("Svc").setHandlerName("h"))
          .build()
  val session = PartitionSession(factory, PRODUCER_ID, options, control, retry, listener)

  /** The most recently opened fake producer. */
  val producer: FakeExactlyOnceProducer
    get() = factory.producers.last()

  /** Start the session and let it reach steady state (connected, or given up). */
  suspend fun connect() {
    session.start(scope)
    delay(100.milliseconds)
  }
}

private class FakeFactory : ProducerFactory {
  val producers = mutableListOf<FakeExactlyOnceProducer>()
  val producerIds = mutableListOf<String>()
  var createCount = 0
  var failOpens = 0

  override fun newExactlyOnceProducer(
      producerId: String,
      options: ProducerOptions,
  ): ExactlyOnceProducer {
    createCount++
    producerIds.add(producerId)
    if (failOpens > 0) {
      failOpens--
      throw RuntimeException("open failed")
    }
    return FakeExactlyOnceProducer().also { producers.add(it) }
  }
}

private class FakeExactlyOnceProducer : ExactlyOnceProducer {
  val sentOffsets = mutableListOf<Long>()
  private val acks = LinkedHashMap<Long, CompletableFuture<SendResult>>()

  /** When false, [trySend] reports backpressure and exposes [readyFuture]. */
  var writable = true
  var readyFuture: CompletableFuture<Void?>? = null

  private var lastSent = -1L
  private var lastAcked = -1L
  private var closed = false

  override fun trySend(offset: Long, invocation: Invocation): SendAttempt {
    check(!closed) { "producer is closed" }
    if (!writable) {
      val ready = CompletableFuture<Void?>().also { readyFuture = it }
      @Suppress("UNCHECKED_CAST")
      return SendAttempt.Backpressured(ready as CompletableFuture<Void>)
    }
    sentOffsets.add(offset)
    lastSent = offset
    val ack = CompletableFuture<SendResult>()
    acks[offset] = ack
    return SendAttempt.Accepted(ack)
  }

  /** Simulate durable commit of [offset]. */
  fun ack(offset: Long) {
    lastAcked = offset
    acks[offset]?.complete(SendResult { offset })
  }

  /** Fail a single in-flight ack (a record-scoped error). */
  fun failAck(offset: Long, cause: IntegrationClientException) {
    acks[offset]?.completeExceptionally(cause)
  }

  /** Simulate a stream failure: fail every pending future. */
  fun failStream(cause: IntegrationClientException) {
    closed = true
    acks.values.forEach { it.completeExceptionally(cause) }
    readyFuture?.completeExceptionally(cause)
  }

  override fun send(offset: Long, invocation: Invocation): CompletableFuture<SendResult> =
      throw UnsupportedOperationException("blocking send is not used by PartitionSession")

  override fun lastSentOffset(): Long = lastSent

  override fun lastAcknowledgedOffset(): Long = lastAcked

  override fun waitAcknowledged(offset: Long): CompletableFuture<Long> =
      CompletableFuture.completedFuture(lastAcked)

  override fun flush(): Long = lastAcked

  override fun flushAsync(): CompletableFuture<Long> = CompletableFuture.completedFuture(lastAcked)

  override fun close() {
    closed = true
  }
}

private class FakeInboundStreamController : PartitionSession.InboundStreamController {
  var pauseCount = 0
  var resumeCount = 0
  val acked = mutableListOf<Long>()
  val rewinds = mutableListOf<Long>()

  override fun pause() {
    pauseCount++
  }

  override fun resume() {
    resumeCount++
  }

  override fun ack(lastCommitted: Long) {
    acked.add(lastCommitted)
  }

  override fun rewindToOffset(offset: Long) {
    rewinds.add(offset)
  }
}

private class RecordingListener : PartitionSession.Listener {
  var closedCount = 0

  override suspend fun onSessionClosed() {
    closedCount++
  }
}
