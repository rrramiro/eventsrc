package io.atlassian.event
package stream

import org.joda.time.DateTime
import scalaz.Equal
import scalaz.effect.IO
import scalaz.syntax.equal._

/**
 * Event wraps the event payload with common information (event id and time of the event)
 */
case class Event[KK, S, E](id: EventId[KK, S], time: DateTime, operation: E) {
  // TODO: Monocle
  def updateId[LL, T](f: EventId[KK, S] => EventId[LL, T]): Event[LL, T, E] =
    Event(f(id), time, operation)
}

object Event {
  def next[KK, S: Sequence, E](key: KK, seq: Option[S], op: E): IO[Event[KK, S, E]] =
    IO { DateTime.now }.map { now =>
      Event(EventId(key, seq.map { Sequence[S].next }.getOrElse { Sequence[S].first }), now, op)
    }

  def at[KK, S, E](e: Event[KK, S, E]): (S, DateTime) =
    (e.id.seq, e.time)

  implicit def eventEqual[KK: Equal, S: Equal, E: Equal]: Equal[Event[KK, S, E]] =
    new Equal[Event[KK, S, E]] {
      def equal(a1: Event[KK, S, E], a2: Event[KK, S, E]): Boolean =
        a1.id === a2.id && a1.time == a2.time && a1.operation === a2.operation
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

