package io.atlassian.event.stream

import scalaz.{ Monad, OptionT, Order, Semigroup, \/ }
import scalaz.stream.Process

/**
 * A rewritable source of events.
 *
 * This should not be used in the general case. Rewriting events violates the immutability of the stream and introduces
 * significant complication.
 */
trait RewritableEventStorage[F[_], K, S, E] extends EventStorage[F, K, S, E] {
  /**
   * Rewrite `oldEvent` to `newEvent`.
   *
   * The keys should match.
   *
   * @return Either an Error or the event that was saved. Other non-specific errors should be available
   *         through the container F.
   */
  def rewrite(oldEvent: Event[K, S, E], newEvent: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]]
}

object RewritableEventStorage {
  implicit def RewritableEventStorageSemigroup[F[_]: Monad, K, S: Order, E](implicit eventStorageSemigroup: Semigroup[EventStorage[F, K, S, E]]): Semigroup[RewritableEventStorage[F, K, S, E]] =
    new Semigroup[RewritableEventStorage[F, K, S, E]] {
      override def append(primary: RewritableEventStorage[F, K, S, E], secondary: => RewritableEventStorage[F, K, S, E]): RewritableEventStorage[F, K, S, E] =
        new RewritableEventStorage[F, K, S, E] {
          override def rewrite(oldEvent: Event[K, S, E], newEvent: Event[K, S, E]): F[\/[EventStreamError, Event[K, S, E]]] =
            primary.rewrite(oldEvent, newEvent)

          override def get(key: K, fromSeq: Option[S]): Process[F, Event[K, S, E]] =
            eventStorageSemigroup.append(primary, secondary).get(key, fromSeq)

          override def put(event: Event[K, S, E]): F[\/[EventStreamError, Event[K, S, E]]] =
            eventStorageSemigroup.append(primary, secondary).put(event)

          override def latest(key: K): OptionT[F, Event[K, S, E]] =
            eventStorageSemigroup.append(primary, secondary).latest(key)
        }
    }
}
