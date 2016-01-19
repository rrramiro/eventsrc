package io.atlassian.event.stream.dynamo

import io.atlassian.aws.dynamodb.{ Column, NamedColumn }
import io.atlassian.event.stream.{ Event, EventId }
import org.joda.time.DateTime

case class EventSourceColumns[KK, S, E](
    key: Column[KK],
    hash: NamedColumn[KK],
    range: NamedColumn[S],
    value: Column[E]
) {
  type EID = EventId[KK, S]
  type EV = Event[KK, S, E]

  private object EID {
    def apply(k: KK, s: S): EID = EventId[KK, S](k, s)

    def unapply(e: EID): Option[(KK, S)] = EventId.unapply[KK, S](e)
  }

  lazy val eventId = Column.compose2[EID](hash.column, range.column) { case EID(k, s) => (k, s) } { case (k, s) => EID(k, s) }

  lazy val event = Column.compose3[EV](eventId.liftOption, Column[DateTime]("LastModifiedTimestamp").column, value) {
    case Event(id, ts, tx) => (None, ts, tx)
  } {
    case (Some(id), ts, tx) => Event(id, ts, tx)
    case (None, _, _)       => ??? // Shouldn't happen, it means there is no event Id in the row
  }

}
