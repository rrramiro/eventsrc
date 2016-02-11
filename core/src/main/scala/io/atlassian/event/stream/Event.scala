package io.atlassian.event
package stream

import org.joda.time.DateTime
import scalaz.{ Order, Ordering }
import scalaz.syntax.order._

/**
 * Event wraps the event payload with common information (event id and time of the event)
 */
case class Event[KK, S, E](id: EventId[KK, S], time: DateTime, operation: E) {
  // TODO: Monocle
  def updateId[LL, T](f: EventId[KK, S] => EventId[LL, T]): Event[LL, T, E] =
    Event(f(id), time, operation)
}

object Event {
  def next[KK, S: Sequence, E](key: KK, seq: Option[S], op: E): Event[KK, S, E] =
    Event(EventId(key, seq.map { Sequence[S].next }.getOrElse { Sequence[S].first }), DateTime.now, op)

  def at[KK, S, E](e: Event[KK, S, E]): (S, DateTime) =
    (e.id.seq, e.time)

  implicit def eventOrder[KK, S: Order, E]: Order[Event[KK, S, E]] =
    new Order[Event[KK, S, E]] {
      def order(x: Event[KK, S, E], y: Event[KK, S, E]): Ordering =
        x.id.seq ?|? y.id.seq
    }

  object syntax {
    implicit class EventSyntax[KK, S, E](val e: Event[KK, S, E]) extends AnyVal {
      def at: (S, DateTime) =
        Event.at(e)

      def process[K, V](s: Snapshot[S, V])(f: Option[V] => PartialFunction[E, Option[V]]): Snapshot[S, V] =
        f(s.value).applyOrElse(e.operation, { (_: E) => s.value }) match {
          case None    => Snapshot.deleted.tupled(e.at)
          case Some(v) => Snapshot.value(v).tupled(e.at)
        }
    }
  }
}

