package io.atlassian.event.stream.unsafe

import io.atlassian.event.stream.{ Event, EventStreamError }

import scalaz.\/

/**
 * A rewritable source of events.
 *
 * This should not be used in the general case. Rewriting events violates the immutability of the stream and introduces
 * significant complication.
 */
trait RewritableEventStorage[F[_], K, S, E] {
  /**
   * Rewrite `oldEvent` to `newEvent`.
   *
   * The keys should match.
   *
   * @return Either an Error or the event that was saved. Other non-specific errors should be available
   *         through the container F.
   */
  def unsafeRewrite(oldEvent: Event[K, S, E], newEvent: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]]
}
