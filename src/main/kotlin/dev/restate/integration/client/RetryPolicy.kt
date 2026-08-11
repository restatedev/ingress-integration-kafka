package dev.restate.integration.client

import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Fully-described retry schedule for (re)connecting an ingestion stream: a bounded exponential
 * backoff. Shaped after Restate's `InvocationRetryPolicy`, minus its terminal action — here,
 * exhausting [maxAttempts] always escalates the failure to the stream's owner (see
 * [ReconnectingIngestionStream]'s `onGiveUp`).
 *
 * Immutable configuration. Consume a run via [iterator]: each call starts a fresh [Iterator] whose
 * [Iterator.next] yields the delay to wait before the next attempt, or `null` once [maxAttempts] is
 * reached. The delay before the n-th attempt is `initialInterval * exponentiationFactor^(n-1)`,
 * clamped to [maxInterval].
 *
 * @param initialInterval delay before the first retry.
 * @param exponentiationFactor multiplier applied to the previous delay for each subsequent retry.
 * @param maxInterval upper bound for any computed delay.
 * @param maxAttempts max *consecutive* connection attempts (initial included) before giving up;
 *   `null` = retry indefinitely ([Iterator.next] never returns `null`). The schedule is restarted
 *   once a connection is proven healthy again, so this bounds a run of consecutive failures, not
 *   the lifetime total.
 */
data class RetryPolicy(
    val initialInterval: Duration = 500.milliseconds,
    val exponentiationFactor: Double = 2.0,
    val maxInterval: Duration = 30.seconds,
    val maxAttempts: Int? = 15 /* roughly 5 minutes */,
) {
  init {
    require(initialInterval > Duration.ZERO) {
      "initialInterval must be positive, was $initialInterval"
    }
    require(exponentiationFactor >= 1.0) {
      "exponentiationFactor must be >= 1.0, was $exponentiationFactor"
    }
    require(maxInterval >= initialInterval) {
      "maxInterval ($maxInterval) must be >= initialInterval ($initialInterval)"
    }
    require(maxAttempts == null || maxAttempts >= 1) {
      "maxAttempts must be >= 1 when set, was $maxAttempts"
    }
  }

  /** Delay before the [attempt]-th connection attempt (1-based; attempt 1 = [initialInterval]). */
  fun retryDelay(attempt: Int): Duration {
    require(attempt >= 1) { "attempt must be >= 1, was $attempt" }
    val scaled = initialInterval * exponentiationFactor.pow(attempt - 1)
    return minOf(scaled, maxInterval)
  }

  /** Start a fresh run of this schedule. */
  fun iterator(): Iterator = Iterator()

  /**
   * A single stateful run of the enclosing [RetryPolicy].
   *
   * The initial connection counts as attempt 1 (already made by the caller before the first
   * failure). [next] returns the delay to wait before the next connection attempt, or `null` once
   * [maxAttempts] is reached — signalling the caller to give up. [attempts] is the number of
   * attempts represented so far (initial included), i.e. what to report when giving up.
   */
  inner class Iterator {
    private var attempt = 1

    /** Connection attempts represented so far, counting the initial one. */
    val attempts: Int
      get() = attempt

    /** Delay before the next attempt, or `null` if the policy is exhausted. */
    fun next(): Duration? {
      if (maxAttempts != null && attempt >= maxAttempts) return null
      val delay = retryDelay(attempt)
      // Guard the unbounded case against Int overflow; the delay has saturated at maxInterval by
      // now.
      if (attempt < Int.MAX_VALUE) attempt += 1
      return delay
    }
  }
}
