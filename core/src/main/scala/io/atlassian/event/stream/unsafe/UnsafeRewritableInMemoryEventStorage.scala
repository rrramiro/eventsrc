package io.atlassian
package event
package stream
package unsafe

import scalaz.{-\/, NonEmptyList, \/, \/- }
import scalaz.concurrent.Task

/**
  * An in-memory UnsafeRewritableEventStorage.
  */
trait UnsafeRewritableInMemoryEventStorage[KK, S, E] extends UnsafeRewritableEventStorage[Task, KK, S, E]

object UnsafeRewritableInMemoryEventStorage {
  type Storage[KK, S, E] = Map[KK, List[Event[KK, S, E]]]

  def apply[KK, S: Sequence, E](map: Storage[KK, S, E]) = Task.delay {
    new UnsafeRewritableInMemoryEventStorage[KK, S, E] {
      override def unsafeRewrite(oldEvent: Event[KK, S, E], newEvent: Event[KK, S, E]): Task[\/[EventStreamError, Event[KK, S, E]]] =
        Task.now {
          (oldEvent, newEvent) match {
            case (o, n) if o.id != n.id => -\/(EventStreamError.reject(NonEmptyList(Reason("Events don't match"))))
            case (o, _) if !map.contains(o.id.key) => -\/(EventStreamError.reject(NonEmptyList(Reason("No userbase stream with that Id"))))
            case (o, n) => replaceOrError(map.get(n.id.key).get, newEvent).map(_ => newEvent)
          }
        }

      /**
        * Replaces an event in a list of events based on the sequence number.
        *
        * @param events The list of events to go through
        * @param newEvent The event to sub in
        * @return The final list of events
        */
      private def replaceOrError(events: List[Event[KK, S, E]], newEvent: Event[KK, S, E]): \/[EventStreamError, List[Event[KK, S, E]]] =
        events.span(e => e.id.seq == newEvent.id.seq) match {
          case (a, Nil) => -\/(EventStreamError.reject(NonEmptyList(Reason("Seq doesn't exist in userbase"))))
          case (a, b) => \/-(a ++ (newEvent :: b.tail))
        }
    }
  }
}
