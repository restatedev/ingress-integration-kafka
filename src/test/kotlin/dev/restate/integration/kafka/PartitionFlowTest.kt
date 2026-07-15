package dev.restate.integration.kafka

import dev.restate.ingestion.v1.Record
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PartitionFlowTest {

  private val sent = mutableListOf<Long>()
  private var pauses = 0
  private var resumes = 0
  private var writeFull = false

  private fun newFlow() =
      PartitionFlow(
          send = { sent.add(it.offset) },
          pause = { pauses++ },
          resume = { resumes++ },
          writeQueueFull = { writeFull },
      )

  private fun rec(offset: Long): Record = Record.newBuilder().setOffset(offset).build()

  @Test
  fun `does not send until credit is granted`() {
    val flow = newFlow()
    flow.offer(rec(0))
    flow.offer(rec(1))

    assertThat(sent).isEmpty()
    assertThat(flow.bufferedCount()).isEqualTo(2)
    // starts paused; no redundant pause emitted while already paused
    assertThat(pauses).isZero()
  }

  @Test
  fun `grants of credit flush buffered records in order`() {
    val flow = newFlow()
    flow.offer(rec(0))
    flow.offer(rec(1))
    flow.offer(rec(2))

    flow.addCredits(2)

    assertThat(sent).containsExactly(0L, 1L)
    assertThat(flow.bufferedCount()).isEqualTo(1)
    assertThat(flow.credits).isZero()
    // still have backlog + no credit -> remains paused (no resume)
    assertThat(resumes).isZero()
    assertThat(flow.lastSentOffset).isEqualTo(1L)
  }

  @Test
  fun `resumes fetching when credit remains and backlog is drained`() {
    val flow = newFlow()
    flow.addCredits(5) // empty buffer, spare credit -> should resume Kafka

    assertThat(resumes).isEqualTo(1)
    assertThat(flow.isPaused()).isFalse()

    flow.offer(rec(0))
    flow.offer(rec(1))
    assertThat(sent).containsExactly(0L, 1L)
    assertThat(flow.credits).isEqualTo(3L)
  }

  @Test
  fun `pauses when credit is exhausted`() {
    val flow = newFlow()
    flow.addCredits(2)
    assertThat(resumes).isEqualTo(1) // resumed on first credit

    flow.offer(rec(0))
    flow.offer(rec(1)) // exhausts credit
    assertThat(sent).containsExactly(0L, 1L)
    assertThat(flow.credits).isZero()
    assertThat(flow.isPaused()).isTrue()
    assertThat(pauses).isEqualTo(1)
  }

  @Test
  fun `buffers overflow beyond credit and flushes on next window`() {
    val flow = newFlow()
    flow.addCredits(1)
    // Simulate a poll batch of 3 arriving while only 1 credit is available.
    flow.offer(rec(10))
    flow.offer(rec(11))
    flow.offer(rec(12))

    assertThat(sent).containsExactly(10L)
    assertThat(flow.bufferedCount()).isEqualTo(2)

    flow.addCredits(5)
    assertThat(sent).containsExactly(10L, 11L, 12L)
    assertThat(flow.bufferedCount()).isZero()
  }

  @Test
  fun `honours sink backpressure and resumes on drain`() {
    val flow = newFlow()
    flow.addCredits(10)
    resumes = 0 // ignore the initial resume

    writeFull = true
    flow.offer(rec(0)) // sink full -> should not send, should pause
    assertThat(sent).isEmpty()
    assertThat(flow.isPaused()).isTrue()
    assertThat(pauses).isEqualTo(1)

    writeFull = false
    flow.onDrain() // buffer flushes, credit remains -> resume
    assertThat(sent).containsExactly(0L)
    assertThat(resumes).isEqualTo(1)
  }
}
