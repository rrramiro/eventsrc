package io.atlassian.event
package stream

import monocle.PLens
import monocle.macros.PLenses
import org.joda.time.DateTime

import scalaz.{ Equal, Functor, Ordering }
import scalaz.syntax.bifunctor._
import scalaz.syntax.equal._

/**
 * Event wraps the event payload with common information (event id and time of the event)
 */
@PLenses case class Event[K, S, A](id: EventId[K, S], time: DateTime, operation: A) {
  def updateId[KK, SS](f: EventId[K, S] => EventId[KK, SS]): Event[KK, SS, A] =
    Event.id.modify(f)(this)

  def bimap[KK, SS](f: K => KK, g: S => SS) =
    updateId { _.bimap[KK, SS](f, g) }
}

object Event {
  def next[K, S: Sequence, E](key: K, seq: Option[S], op: E): Event[K, S, E] =
    Event(EventId(key, seq.map { Sequence[S].next }.getOrElse { Sequence[S].first }), DateTime.now, op)

  def at[K, S, E](e: Event[K, S, E]): (S, DateTime) =
    (e.id.seq, e.time)

  implicit def eventEqual[K: Equal, S: Equal, A: Equal]: Equal[Event[K, S, A]] =
    new Equal[Event[K, S, A]] {
      def equal(a1: Event[K, S, A], a2: Event[K, S, A]): Boolean =
        a1.id === a2.id && a1.time == a2.time && a1.operation === a2.operation
    }

  implicit def EventFunctor[K, S]: Functor[Event[K, S, ?]] =
    new Functor[Event[K, S, ?]] {
      override def map[A, B](ev: Event[K, S, A])(f: A => B): Event[K, S, B] =
        Event.operation[K, S, A, B].modify(f)(ev)
    }

  object syntax {
    implicit class EventSyntax[K, S, A](val e: Event[K, S, A]) extends AnyVal {
      def at: (S, DateTime) =
        Event.at(e)

      def process[K, V](s: Snapshot[S, V])(f: Option[V] => PartialFunction[A, Option[V]]): Snapshot[S, V] =
        f(s.value).applyOrElse(e.operation, { (_: A) => s.value }) match {
          case None    => Snapshot.deleted.tupled(e.at)
          case Some(v) => Snapshot.value(v).tupled(e.at)
        }
    }
  }
}

