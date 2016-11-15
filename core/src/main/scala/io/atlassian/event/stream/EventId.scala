package io.atlassian.event
package stream

import monocle.macros.PLenses

import scalaz.{ Bifunctor, Equal }
import scalaz.syntax.equal._

/**
 * A event is identified by the key and an incrementing sequence 'number'
 * @param key The key
 * @param seq the sequence number
 */
@PLenses case class EventId[K, S](key: K, seq: S)

object EventId {
  def first[K, S: Sequence](key: K): EventId[K, S] =
    EventId(key, Sequence[S].first)

  def next[K, S: Sequence](id: EventId[K, S]): EventId[K, S] =
    EventId.seq.set(Sequence[S].next(id.seq))(id)

  implicit val EventIdBifunctor: Bifunctor[EventId] =
    new Bifunctor[EventId] {
      def bimap[K, S, KK, SS](id: EventId[K, S])(f: K => KK, g: S => SS): EventId[KK, SS] =
        (EventId.key.modify(f) andThen EventId.seq.modify(g))(id)
    }

  implicit def EventIdEqual[K: Equal, S: Equal]: Equal[EventId[K, S]] =
    new Equal[EventId[K, S]] {
      def equal(a1: EventId[K, S], a2: EventId[K, S]): Boolean =
        a1.key === a2.key && a1.seq === a2.seq
    }
}
