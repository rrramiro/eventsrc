package io.atlassian.event
package stream

import scalaz.{ \/, Functor }
import scalaz.stream.Process
import scalaz.syntax.bifunctor._
import scalaz.syntax.functor._

/**
 * A source of events. Implementations wrap around an underlying data store (e.g. in-memory map or DynamoDB).
 *
 * @tparam F Container around operations on an underlying data store e.g. Task.
 */
trait EventStorage[F[_], K, S, E] { self =>
  /**
   * Retrieve a stream of events from the underlying data store. This stream should take care of pagination and
   * cleanup of any underlying resources (e.g. closing connections if required).
   * @param key The key
   * @param fromSeq The starting sequence to get events from (exclusive). None to get from the start.
   * @return Stream of events.
   */
  def get(key: K, fromSeq: Option[S]): Process[F, Event[K, S, E]]

  /**
   * Save the given event.
   *
   * @return Either an Error or the event that was saved. Other non-specific errors should be available
   *         through the container F.
   */
  def put(event: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]]

  // Invariant bifunctor
  def mapKS[KK, SS](k: KK => K, kk: K => KK, s: SS => S, ss: S => SS)(implicit F: Functor[F]) =
    // TODO: Monocle would clean this up a bit.
    new EventStorage[F, KK, SS, E] {
      def updateKey[E](event: Event[K, S, E]) =
        event.updateId(_.bimap(kk, ss))

      def get(key: KK, fromSeq: Option[SS]) =
        self.get(k(key), fromSeq.map(s)).map(updateKey)

      def put(event: Event[KK, SS, E]) =
        self.put(event.updateId(_.bimap(k, s))).map(_.map(updateKey))
    }
}
