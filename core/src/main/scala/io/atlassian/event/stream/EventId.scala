package io.atlassian.event
package stream

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
}

