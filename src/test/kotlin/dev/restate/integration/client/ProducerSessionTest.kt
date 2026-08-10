package dev.restate.integration.client

import com.google.protobuf.ByteString
import dev.restate.ingestion.v1.Invocation
import dev.restate.integration.client.IntegrationClientException.Kind
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * State-machine tests for [ProducerSession] in isolation — no Vert.x. The session runs on the
 * [runTest] virtual-time dispatcher; the ingestion [IntegrationClient]/[InvocationStream] and the
 * [InboundStreamController] are faked, and server events are simulated by driving the fake stream's
 * budget and firing the captured [InvocationStream.Listener] callbacks.
 *
 * The flow-control window is measured in **bytes** (the serialized invocation size), matching the
 * real [InvocationStreamImpl]. Tests grant windows sized to specific records via [Env.bytes] rather
 * than hardcoding byte counts, so they don't depend on exact proto encoding sizes.
 *
 * We only use stable coroutine-test APIs: [runTest] + [TestScope] + [delay]. A `delay` in the test
 * body advances the shared virtual clock, which lets the session's launched coroutine make progress
 * (and skips its backoff `delay`s instantly) — no `advanceUntilIdle`/experimental scheduler calls.
 */
class ProducerSessionTest {

  // ---- start / connect ----

  @Test
  fun `opens a stream for the producer id, sends Start, and pauses the source`() = test {
    it.connect()

    assertThat(it.client.openedProducerIds).containsExactly(PRODUCER_ID)
    assertThat(it.control.pauseCount).isGreaterThanOrEqualTo(1)
    // The initial settings are handed to the client to stamp onto the Start handshake frame.
    assertThat(it.client.startedSettings).hasSize(1)
    assertThat(it.client.startedSettings.single().service).isEqualTo("Svc")
  }

  @Test
  fun `start is idempotent while running`() = test {
    it.connect()
    it.session.start(it.scope, settings())
    settle()

    assertThat(it.client.openCount).isEqualTo(1)
  }

  // ---- flow control ----

  @Test
  fun `records offered before the first window are buffered, not written`() = test {
    it.connect()

    it.session.offer(record(0))
    it.session.offer(record(1))

    assertThat(it.client.stream.written).isEmpty()
  }

  @Test
  fun `a window flushes the backlog and resumes the source`() = test {
    it.connect()
    it.session.offer(record(0))
    it.session.offer(record(1))

    // Enough bytes for both records, plus slack so the window stays open once the backlog drains.
    it.grantWindow(it.bytes(record(0), record(1)) + 1)

    assertThat(it.client.stream.writtenOffsets()).containsExactly(0L, 1L)
    assertThat(it.control.resumeCount).isEqualTo(1)
  }

  @Test
  fun `a window smaller than the backlog writes up to budget and stays paused`() = test {
    it.connect()
    it.session.offer(record(0))
    it.session.offer(record(1))
    it.session.offer(record(2))

    // Exactly two records' worth of window: after the second the budget hits 0, so the third waits.
    it.grantWindow(it.bytes(record(0), record(1)))

    assertThat(it.client.stream.writtenOffsets()).containsExactly(0L, 1L)
    assertThat(it.control.resumeCount).isEqualTo(0)
  }

  @Test
  fun `a single record may overshoot the window, then the stream blocks`() = test {
    it.connect()

    // A 1-byte window is smaller than any record: it's legal to send one record that overshoots
    // (driving the window negative), after which no more may be sent until the next grant.
    it.grantWindow(1)
    it.session.offer(record(0))
    it.session.offer(record(1)) // window already <= 0 -> buffered

    assertThat(it.client.stream.writtenOffsets()).containsExactly(0L)
    assertThat(it.client.stream.budget).isLessThanOrEqualTo(0)
  }

  @Test
  fun `while writable, offered records are written immediately`() = test {
    it.connect()
    it.grantWindow(it.bytes(record(0), record(1)) + 100) // empty backlog -> resume

    it.session.offer(record(0))
    it.session.offer(record(1))

    assertThat(it.client.stream.writtenOffsets()).containsExactly(0L, 1L)
  }

  @Test
  fun `exhausting the budget pauses the source and buffers further records`() = test {
    it.connect()
    it.grantWindow(it.bytes(record(0), record(1)))

    it.session.offer(record(0))
    it.session.offer(record(1)) // budget hits 0 here
    it.session.offer(record(2)) // buffered, not written

    assertThat(it.client.stream.writtenOffsets()).containsExactly(0L, 1L)
  }

  // ---- ack ----

  @Test
  fun `ack forwards the offset to the source`() = test {
    it.connect()

    it.client.listener.ack(7)

    assertThat(it.control.acked).containsExactly(7L)
  }

  // ---- teardown ----

  @Test
  fun `a clean close ends the stream and notifies the listener once`() = test {
    it.connect()

    it.client.listener.onClose(null)
    settle()

    assertThat(it.client.stream.ended).isTrue()
    assertThat(it.sessionClosed.closedCount).isEqualTo(1)
  }

  @Test
  fun `a NOT_FOUND error tears the session down without reconnecting`() = test {
    it.connect()

    it.client.listener.onClose(IntegrationClientException(Kind.NOT_FOUND))
    settle()

    assertThat(it.sessionClosed.closedCount).isEqualTo(1)
    assertThat(it.client.openCount).isEqualTo(1)
  }

  @Test
  fun `a BAD_REQUEST error tears the session down without reconnecting`() = test {
    it.connect()

    it.client.listener.onClose(IntegrationClientException(Kind.BAD_REQUEST))
    settle()

    assertThat(it.sessionClosed.closedCount).isEqualTo(1)
    assertThat(it.client.openCount).isEqualTo(1)
  }

  @Test
  fun `records offered after close are not written`() = test {
    it.connect()
    it.session.close()
    settle()

    it.session.offer(record(0))

    assertThat(it.client.stream.written).isEmpty()
  }

  @Test
  fun `close cancels the session and notifies the listener once`() = test {
    it.connect()

    it.session.close()
    settle()

    assertThat(it.sessionClosed.closedCount).isEqualTo(1)
  }

  // ---- reconnect ----

  @Test
  fun `a retryable error reconnects without tearing down`() =
      test(fastRetry()) {
        it.connect()

        it.client.listener.onClose(IntegrationClientException(Kind.SHUTTING_DOWN))
        settle()

        assertThat(it.client.openCount).isEqualTo(2)
        assertThat(it.sessionClosed.closedCount).isEqualTo(0)
      }

  @Test
  fun `a GO_AWAY error reconnects without tearing down`() =
      test(fastRetry()) {
        it.connect()

        it.client.listener.onClose(IntegrationClientException(Kind.GO_AWAY))
        settle()

        assertThat(it.client.openCount).isEqualTo(2)
        assertThat(it.sessionClosed.closedCount).isEqualTo(0)
      }

  @Test
  fun `a retryable error after an ack rewinds to the last acked offset`() =
      test(fastRetry()) {
        it.connect()
        it.client.listener.ack(10)

        it.client.listener.onClose(IntegrationClientException(Kind.SHUTTING_DOWN))
        settle()

        assertThat(it.control.rewinds).containsExactly(10L)
      }

  @Test
  fun `a retryable error without a prior ack does not rewind`() =
      test(fastRetry()) {
        it.connect()

        it.client.listener.onClose(IntegrationClientException(Kind.SHUTTING_DOWN))
        settle()

        assertThat(it.control.rewinds).isEmpty()
      }

  @Test
  fun `retries stop and tear down when the policy allows no retries`() =
      test(RetryPolicy(maxAttempts = 1)) {
        it.connect()

        it.client.listener.onClose(IntegrationClientException(Kind.SHUTTING_DOWN))
        settle()

        assertThat(it.sessionClosed.closedCount).isEqualTo(1)
        assertThat(it.client.openCount).isEqualTo(1)
      }

  @Test
  fun `a failed open is retried and can then succeed`() =
      test(fastRetry()) {
        it.client.failOpens = 1

        it.connect()

        assertThat(it.client.openCount).isEqualTo(2)
        assertThat(it.sessionClosed.closedCount).isEqualTo(0)
      }

  @Test
  fun `repeated open failures give up once the policy is exhausted`() =
      test(fastRetry(maxAttempts = 3)) {
        it.client.failOpens = Int.MAX_VALUE // every open fails

        it.connect()

        assertThat(it.client.openCount).isEqualTo(3)
        assertThat(it.sessionClosed.closedCount).isEqualTo(1)
      }

  @Test
  fun `late callbacks from a superseded connection are ignored`() =
      test(fastRetry()) {
        it.connect()
        val stale = it.client.listener // connection #1's listener

        stale.onClose(IntegrationClientException(Kind.SHUTTING_DOWN))
        settle() // reconnects -> connection #2 is now current

        stale.ack(99) // stale connection acks -> must be ignored

        assertThat(it.control.acked).doesNotContain(99L)
      }

  // ---- harness ----

  /** Comfortably longer than any backoff a test uses; delays are virtual, so this is free. */
  private val settleDuration = 100.milliseconds

  /** Advance the shared virtual clock so the session's launched coroutine makes progress. */
  private suspend fun settle() = delay(settleDuration)

  private fun test(retry: RetryPolicy = RetryPolicy(), body: suspend TestScope.(Env) -> Unit) =
      runTest {
        val env = Env(this, retry) { settle() }
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
    Invocation.newBuilder()
        .setOffset(offset)
        .setPayload(ByteString.copyFromUtf8("payload-$offset"))
        .build()

private fun settings(): StreamSettings =
    StreamSettings.newBuilder().setService("Svc").setHandler("h").build()

private fun fastRetry(maxAttempts: Int? = null) =
    RetryPolicy(initialInterval = 1.milliseconds, maxAttempts = maxAttempts)

private class Env(
    val scope: TestScope,
    retry: RetryPolicy,
    private val settle: suspend () -> Unit,
) {
  val client = FakeIntegrationClient()
  val control = FakeInboundStreamController()
  val sessionClosed = RecordingListener()
  val session = ProducerSession(client, PRODUCER_ID, control, retry, sessionClosed)

  /** Start the session and let it reach steady state (connected, or given up). */
  suspend fun connect() {
    session.start(scope, settings())
    settle()
  }

  /**
   * Simulate a server window grant of [increment] bytes on the current connection (synchronous).
   */
  fun grantWindow(increment: Long) {
    client.stream.budget += increment
    client.listener.onWritable(client.stream.budget)
  }

  /** The serialized byte size of [records] combined — the exact window needed to send them all. */
  fun bytes(vararg records: Invocation): Long = records.sumOf { it.serializedSize.toLong() }
}

private class FakeIntegrationClient : IntegrationClient {
  val openedProducerIds = mutableListOf<String>()
  val startedSettings = mutableListOf<StreamSettings>()
  var openCount = 0
  var failOpens = 0

  /** The listener/stream of the most recently opened connection. */
  lateinit var listener: InvocationStream.Listener
  var stream = FakeInvocationStream()

  override suspend fun open(
      producerId: String,
      listener: InvocationStream.Listener,
      initialStreamSettings: StreamSettings,
  ): InvocationStream {
    openCount++
    openedProducerIds.add(producerId)
    if (failOpens > 0) {
      failOpens--
      throw RuntimeException("open failed")
    }
    this.listener = listener
    startedSettings.add(initialStreamSettings)
    return FakeInvocationStream().also { stream = it }
  }

  override suspend fun close() {}
}

private class FakeInvocationStream : InvocationStream {
  /** Remaining window in **bytes**; the test grants these to simulate windows. */
  var budget = 0L
  val written = mutableListOf<Invocation>()
  /** Settings sent mid-stream via [updateSettings]. */
  val settingsSent = mutableListOf<StreamSettings>()
  var ended = false

  fun writtenOffsets(): List<Long> = written.map { it.offset }

  override fun updateSettings(settings: StreamSettings) {
    settingsSent.add(settings)
  }

  override fun write(invocation: Invocation) {
    // Byte-based window, mirroring IngestionStreamImpl: a write may drive the budget negative.
    budget -= invocation.serializedSize.toLong()
    written.add(invocation)
  }

  override fun isWritable(): Boolean = budget > 0

  override fun end() {
    ended = true
  }
}

private class FakeInboundStreamController : ProducerSession.InboundStreamController {
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

private class RecordingListener : ProducerSession.Listener {
  var closedCount = 0

  override suspend fun onSessionClosed() {
    closedCount++
  }
}
