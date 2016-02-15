package io.atlassian.event
package stream

import scalaz.{ Bifunctor, Equal }
import scalaz.syntax.equal._

/**
 * A event is identified by the key and an incrementing sequence 'number'
 * @param key The key
 * @param sequence the sequence number
 */
case class EventId[KK, S](key: KK, seq: S)

object EventId {
  def first[KK, S: Sequence](key: KK): EventId[KK, S] =
    EventId(key, Sequence[S].first)

  def next[KK, S: Sequence](id: EventId[KK, S]): EventId[KK, S] =
    id.copy(seq = Sequence[S].next(id.seq))

  implicit val EventIdBifunctor: Bifunctor[EventId] =
    new Bifunctor[EventId] {
      def bimap[A, B, C, D](fab: EventId[A, B])(f: A => C, g: B => D) =
        EventId(f(fab.key), g(fab.seq))
    }

  implicit def EventIdEqual[A: Equal, B: Equal]: Equal[EventId[A, B]] =
    new Equal[EventId[A, B]] {
      def equal(a1: EventId[A, B], a2: EventId[A, B]): Boolean =
        a1.key === a2.key && a1.seq === a2.seq
    }
}
