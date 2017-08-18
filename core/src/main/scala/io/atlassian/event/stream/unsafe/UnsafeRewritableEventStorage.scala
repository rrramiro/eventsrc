package io.atlassian.event.stream.unsafe

import io.atlassian.event.stream.{ Event, EventStreamError }

import scalaz.{ Functor, \/ }
import scalaz.syntax.functor._

/**
 * A rewritable source of events.
 *
 * This should not be used in the general case. Rewriting events violates the immutability of the stream and introduces
 * significant complication.
 */
trait UnsafeRewritableEventStorage[F[_], K, S, E] { self =>
  /**
   * Rewrite `oldEvent` to `newEvent`.
   *
   * The keys must match. If they do not, then you'll get back an error.
   *
   * @return Either an Error or the event that was saved. Other non-specific errors should be available
   *         through the container F.
   */
  def unsafeRewrite(oldEvent: Event[K, S, E], newEvent: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]]

  // Invariant bifunctor
  def mapKS[KK, SS](k: KK => K, kk: K => KK, s: SS => S, ss: S => SS)(implicit F: Functor[F]) =
    // TODO: Monocle would clean this up a bit.
    new UnsafeRewritableEventStorage[F, KK, SS, E] {
      val updateKey: Event[K, S, E] => Event[KK, SS, E] =
        _.bimap(kk, ss)

      override def unsafeRewrite(oldEvent: Event[KK, SS, E], newEvent: Event[KK, SS, E]): F[\/[EventStreamError, Event[KK, SS, E]]] =
        self.unsafeRewrite(oldEvent.bimap(k, s), newEvent.bimap(k, s)).map { _.map { updateKey } }
    }
}
