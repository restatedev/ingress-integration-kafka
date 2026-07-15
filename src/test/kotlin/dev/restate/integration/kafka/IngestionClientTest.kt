package dev.restate.integration.kafka

import com.google.protobuf.ByteString
import dev.restate.ingestion.v1.ErrorKind
import dev.restate.ingestion.v1.Record
import dev.restate.ingestion.v1.Settings
import dev.restate.integration.kafka.config.IngressEndpoint
import io.vertx.core.Vertx
import io.vertx.grpc.client.GrpcClient
import io.vertx.kotlin.coroutines.coAwait
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class IngestionClientTest {

  private lateinit var vertx: Vertx
  private lateinit var grpcClient: GrpcClient
  private lateinit var server: FakeIngestionServer

  @BeforeEach
  fun setUp() {
    runBlocking {
      vertx = Vertx.vertx()
      grpcClient = GrpcClient.client(vertx)
      server = FakeIngestionServer(vertx).start()
    }
  }

  @AfterEach
  fun tearDown() {
    runBlocking {
      grpcClient.close().coAwait()
      server.close()
      vertx.close().coAwait()
    }
  }

  private fun client() = IngestionClient(grpcClient, IngressEndpoint("localhost", server.port, false))

  // JUnit 6 requires @Test methods to return Unit; the `-> Unit` lambda type discards the
  // trailing assertion value so `= runTest { ... }` stays Unit-returning.
  private fun runTest(block: suspend CoroutineScope.() -> Unit) = runBlocking { block() }

  /** Collects listener callbacks into thread-safe queues for the test thread to await. */
  private class CollectingListener : IngestionStreamListener {
    val windowUpdates = LinkedBlockingQueue<Long>()
    val commits = LinkedBlockingQueue<Long>()
    val errors = LinkedBlockingQueue<dev.restate.ingestion.v1.Error>()
    val closed = CompletableFuture<Throwable?>()

    override fun onWindowUpdate(increment: Long) {
      windowUpdates.add(increment)
    }

    override fun onCommit(lastCommitted: Long) {
      commits.add(lastCommitted)
    }

    override fun onError(error: dev.restate.ingestion.v1.Error) {
      errors.add(error)
    }

    override fun onClose(cause: Throwable?) {
      closed.complete(cause)
    }
  }

  private fun <T> LinkedBlockingQueue<T>.await(): T =
      poll(5, TimeUnit.SECONDS) ?: error("timed out waiting for a value")

  private fun record(offset: Long, key: String, payload: String): Record =
      Record.newBuilder()
          .setOffset(offset)
          .setKey(key)
          .setPayload(ByteString.copyFromUtf8(payload))
          .build()

  @Test
  fun `sends settings and receives the initial window grant`() = runTest {
    server.onSettings = { _, resp ->
      resp.write(
          dev.restate.ingestion.v1.Response.newBuilder()
              .setAck(dev.restate.ingestion.v1.WindowUpdate.newBuilder().setIncrement(10))
              .build())
    }
    val listener = CollectingListener()
    val stream = client().open(listener)

    stream.sendSettings(
        Settings.newBuilder().setProducerId("g/topic/0").setService("Svc").setHandler("h").build())

    assertThat(listener.windowUpdates.await()).isEqualTo(10L)
    // server actually saw our settings
    assertThat(server.settingsReceived).hasSize(1)
    assertThat(server.settingsReceived[0].producerId).isEqualTo("g/topic/0")
  }

  @Test
  fun `sends records and receives commit watermarks`() = runTest {
    server.onSettings = { _, resp ->
      resp.write(
          dev.restate.ingestion.v1.Response.newBuilder()
              .setAck(dev.restate.ingestion.v1.WindowUpdate.newBuilder().setIncrement(100))
              .build())
    }
    // Ack + commit each record's offset as it arrives.
    server.onRecord = { rec, resp ->
      resp.write(
          dev.restate.ingestion.v1.Response.newBuilder()
              .setLastCommitted(rec.offset)
              .setAck(dev.restate.ingestion.v1.WindowUpdate.newBuilder().setIncrement(1))
              .build())
    }

    val listener = CollectingListener()
    val stream = client().open(listener)
    stream.sendSettings(Settings.newBuilder().setProducerId("g/topic/0").build())
    assertThat(listener.windowUpdates.await()).isEqualTo(100L)

    stream.sendRecord(record(0, "k0", "v0"))
    stream.sendRecord(record(1, "k1", "v1"))

    assertThat(listener.commits.await()).isEqualTo(0L)
    assertThat(listener.commits.await()).isEqualTo(1L)
    assertThat(server.recordsReceived).hasSize(2)
    assertThat(server.recordsReceived.map { it.key }).containsExactly("k0", "k1")
    assertThat(server.recordsReceived[0].payload.toStringUtf8()).isEqualTo("v0")
  }

  @Test
  fun `propagates server errors to the listener`() = runTest {
    server.onSettings = { _, _ -> server.sendError(ErrorKind.ERROR_KIND_UNKNOWN_SERVICE, "no such service") }

    val listener = CollectingListener()
    val stream = client().open(listener)
    stream.sendSettings(Settings.newBuilder().setProducerId("g/topic/0").setService("Missing").build())

    val error = listener.errors.await()
    assertThat(error.kind).isEqualTo(ErrorKind.ERROR_KIND_UNKNOWN_SERVICE)
    assertThat(error.message).contains("no such service")
  }

  @Test
  fun `notifies listener when the server ends the stream`() = runTest {
    server.onSettings = { _, _ -> server.endStream() }
    val listener = CollectingListener()
    val stream = client().open(listener)
    stream.sendSettings(Settings.newBuilder().setProducerId("g/topic/0").build())

    val cause = listener.closed.get(5, TimeUnit.SECONDS)
    assertThat(cause).isNull()
  }
}
