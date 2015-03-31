package io.atlassian.event
package stream

import org.joda.time.DateTime

/**
 * Event wraps the event payload with common information (event id and time of the event)
 */
case class Event[KK, S, E](id: EventId[KK, S], time: DateTime, operation: E)

object Event {
  def next[KK, S: Sequence, E](key: KK, seq: Option[S], op: E): Event[KK, S, E] =
    Event(EventId(key, seq.map { Sequence[S].next }.getOrElse { Sequence[S].first }), DateTime.now, op)
}

