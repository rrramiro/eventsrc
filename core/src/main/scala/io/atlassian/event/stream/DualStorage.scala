package io.atlassian.event
package stream

import scalaz.stream.{ Process, Tee, tee }
import scalaz.syntax.order._
import scalaz.syntax.plus._
import scalaz.{ \/, Monad, OptionT, Order }

object DualStorage {
  def combine[F[_]: Monad, K, S: Order, E](primary: EventStorage[F, K, S, E], secondary: EventStorage[F, K, S, E]): EventStorage[F, K, S, E] =
    new EventStorage[F, K, S, E] {
      private type EventTee = Tee[Event[K, S, E], Event[K, S, E], Event[K, S, E]]

      private def restL(s: Option[S], x: Option[Event[K, S, E]]): EventTee =
        tee.feedL(x.toSeq)(tee.passL).dropWhile { leftE => s.fold(false)(_ >= leftE.id.seq) }

      private def restR(s: Option[S], x: Option[Event[K, S, E]]): EventTee =
        tee.feedR(x.toSeq)(tee.passR).dropWhile { rightE => s.fold(false)(_ >= rightE.id.seq) }

      private def merge(fromSeq: Option[S], left: Option[Event[K, S, E]], right: Option[Event[K, S, E]]): EventTee = {
        (fromSeq, left, right) match {
          // when the left slot is empty, await an element from the left.
          // if the left branch has terminated, forward the rest of the right branch.
          case (_, None, _) => tee.receiveLOr(restR(fromSeq, right)) { leftE =>
            merge(fromSeq, Some(leftE), right)
          }
          // when the right slot is empty, await an element from the right.
          // if the right branch has terminated, forward the rest of the left branch.
          case (_, _, None) => tee.receiveROr(restL(fromSeq, left)) { rightE =>
            merge(fromSeq, left, Some(rightE))
          }
          // if the left slot has an event older than the current sequence, discard it.
          case (Some(seq), Some(leftE), _) if seq >= leftE.id.seq =>
            merge(fromSeq, None, right)
          // if the right slot has an event older than the current sequence, discard it.
          case (Some(seq), _, Some(rightE)) if seq >= rightE.id.seq =>
            merge(fromSeq, left, None)
          // if both slots contain events, emit the earlier one.
          case (_, Some(leftE), Some(rightE)) => if (leftE.id.seq <= rightE.id.seq) {
            Process.emit(leftE).append(merge(Some(leftE.id.seq), None, right))
          }
          else {
            Process.emit(rightE).append(merge(Some(rightE.id.seq), left, None))
          }
        }
      }

      def get(key: K, fromSeq: Option[S]): Process[F, Event[K, S, E]] = {
        val primaryGet = primary.get(key, fromSeq)
        val secondaryGet = secondary.get(key, fromSeq)
        primaryGet.tee(secondaryGet)(merge(None, None, None))
      }

      def latest(key: K): OptionT[F, Event[K, S, E]] =
        primary.latest(key) <+> secondary.latest(key)

      def put(event: Event[K, S, E]): F[EventStreamError \/ Event[K, S, E]] =
        primary.put(event)
    }
}
