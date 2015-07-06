package io.atlassian.event
package stream

import scalaz.Bifunctor

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

  implicit val EventIdBifunctor = new Bifunctor[EventId] {
    def bimap[A, B, C, D](fab: EventId[A, B])(f: A => C, g: B => D) =
      EventId(f(fab.key), g(fab.seq))
  }
}
