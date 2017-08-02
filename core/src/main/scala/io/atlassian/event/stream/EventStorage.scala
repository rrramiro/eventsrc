package io.atlassian.event
package stream

import scalaz.{ Functor, InvariantFunctor, Monad, OptionT, Order, Semigroup, \/ }
import scalaz.stream.{ Process, Tee, tee }
import scalaz.syntax.bifunctor._
import scalaz.syntax.applicative._
import scalaz.syntax.order._
import scalaz.syntax.either._

/**
 * A source of events. Implementations wrap around an underlying data store (e.g. in-memory map or DynamoDB).
 *
 * @tparam F Container around operations on an underlying data store e.g. Task.
 */
trait EventStorage[F[_], K, S, E] { self =>
  /**
   * Retrieve a stream of events from the underlying data store. This stream should take care of pagination and
   * cleanup of any underlying resources (e.g. closing connections if required). Must be in order of sequence.
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

  /**
    * Rewrite `oldEvent` to `newEvent`.
    *
    * The keys should match.
    *
    * @return Either an Error or the event that was saved. Other non-specific errors should be available
    *         through the container F.
    */
  def rewrite(oldEvent: Event[K, S, E], newEvent: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]] =
    if (oldEvent.id.equals(newEvent.id))
      rewriteEvent(oldEvent, newEvent)
    else
      (EventStreamError.EventIdsDoNotMatch: EventStreamError).left[Event[K, S, E]].point[F]

  protected def rewriteEvent(oldEvent: Event[K, S, E], newEvent: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]]

  /**
   * Get the latest event.
   *
   * @param key The key
   * @return Single event if found.
   */
  def latest(key: K): OptionT[F, Event[K, S, E]]

  // Invariant bifunctor
  def mapKS[KK, SS](k: KK => K, kk: K => KK, s: SS => S, ss: S => SS)(implicit F: Functor[F]) =
    // TODO: Monocle would clean this up a bit.
    new EventStorage[F, KK, SS, E] {
      val updateKey: Event[K, S, E] => Event[KK, SS, E] =
        _.bimap(kk, ss)

      def get(key: KK, fromSeq: Option[SS]) =
        self.get(k(key), fromSeq.map{ s }).map { updateKey }

      def put(event: Event[KK, SS, E]) =
        self.put(event.bimap(k, s)).map { _.map { updateKey } }

      def latest(key: KK) =
        self.latest(k(key)).map { updateKey }
    }
}

object EventStorage {
  def maxBy[A, B: Order](f: A => B)(a1: A, a2: A): A =
    if (f(a1) >= f(a2)) a1 else a2

  def EventStorageKeyInvariantFunctor[F[_]: Functor, S, E]: InvariantFunctor[EventStorage[F, ?, S, E]] =
    new InvariantFunctor[EventStorage[F, ?, S, E]] {
      override def xmap[K, KK](ma: EventStorage[F, K, S, E], f: K => KK, g: KK => K): EventStorage[F, KK, S, E] =
        ma.mapKS[KK, S](g, f, identity, identity)
    }

  // Not only a Semigroup but also a Band:
  //     forall a. a |+| a = a
  implicit def EventStorageSemigroup[F[_]: Monad, K, S: Order, E]: Semigroup[EventStorage[F, K, S, E]] =
    new Semigroup[EventStorage[F, K, S, E]] {
      def append(primary: EventStorage[F, K, S, E], secondary: => EventStorage[F, K, S, E]): EventStorage[F, K, S, E] =
        new EventStorage[F, K, S, E] {
          private type EventTee = Tee[Event[K, S, E], Event[K, S, E], Event[K, S, E]]

          private def restL(s: Option[S], x: Option[Event[K, S, E]]): EventTee =
            tee.feedL(x.toSeq)(tee.passL).dropWhile { leftE => s.fold(false)(_ >= leftE.id.seq) }

          private def restR(s: Option[S], x: Option[Event[K, S, E]]): EventTee =
            tee.feedR(x.toSeq)(tee.passR).dropWhile { rightE => s.fold(false)(_ >= rightE.id.seq) }

          private def merge(fromSeq: Option[S], left: Option[Event[K, S, E]], right: Option[Event[K, S, E]]): EventTee =
            (fromSeq, left, right) match {
              // when the left slot is empty, await an element from the left.
              // if the left branch has terminated, forward the rest of the right branch.
              case (_, None, _) =>
                tee.receiveLOr(restR(fromSeq, right)) { leftE =>
                  merge(fromSeq, Some(leftE), right)
                }
              // when the right slot is empty, await an element from the right.
              // if the right branch has terminated, forward the rest of the left branch.
              case (_, _, None) =>
                tee.receiveROr(restL(fromSeq, left)) { rightE =>
                  merge(fromSeq, left, Some(rightE))
                }
              // if the left slot has an event older than the current sequence, discard it.
              case (Some(seq), Some(leftE), _) if seq >= leftE.id.seq =>
                merge(fromSeq, None, right)
              // if the right slot has an event older than the current sequence, discard it.
              case (Some(seq), _, Some(rightE)) if seq >= rightE.id.seq =>
                merge(fromSeq, left, None)
              // if both slots contain events, emit the earlier one.
              case (_, Some(leftE), Some(rightE)) =>
                if (leftE.id.seq <= rightE.id.seq)
                  Process.emit(leftE).append(merge(Some(leftE.id.seq), None, right))
                else
                  Process.emit(rightE).append(merge(Some(rightE.id.seq), left, None))
            }

          def get(key: K, fromSeq: Option[S]): Process[F, Event[K, S, E]] = {
            val primaryGet = primary.get(key, fromSeq)
            val secondaryGet = secondary.get(key, fromSeq)
            primaryGet.tee(secondaryGet)(merge(None, None, None))
          }

          def latest(key: K): OptionT[F, Event[K, S, E]] =
            (primary.latest(key) |@| secondary.latest(key))(maxBy[Event[K, S, E], S](_.id.seq))

          def put(event: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]] =
            primary.put(event)
        }
    }
}
