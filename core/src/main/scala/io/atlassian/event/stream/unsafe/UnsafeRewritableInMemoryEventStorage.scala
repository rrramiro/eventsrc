package io.atlassian
package event
package stream
package unsafe

import io.atlassian.event.stream.memory.MemoryEventStorage.{ Storage, Stream => EventStream }

import scala.collection.concurrent.TrieMap
import scalaz.{ -\/, Equal, NonEmptyList, \/, \/- }
import scalaz.concurrent.Task
import scalaz.syntax.equal._

/**
 * An in-memory UnsafeRewritableEventStorage.
 */
trait UnsafeRewritableInMemoryEventStorage[KK, S, E] extends UnsafeRewritableEventStorage[Task, KK, S, E]

object UnsafeRewritableInMemoryEventStorage {
  def apply[KK, S: Sequence, E](store: Storage[KK, S, E]): UnsafeRewritableInMemoryEventStorage[KK, S, E] =
    new UnsafeRewritableInMemoryEventStorage[KK, S, E] {
      override def unsafeRewrite(oldEvent: Event[KK, S, E], newEvent: Event[KK, S, E]): Task[\/[EventStreamError, Event[KK, S, E]]] =
        Task.now {
          store.synchronized {
            (oldEvent, newEvent) match {
              case (o, n) if o.id != n.id              => -\/(EventStreamError.reject(NonEmptyList(Reason("Events don't match"))))
              case (o, _) if !store.contains(o.id.key) => -\/(EventStreamError.reject(NonEmptyList(Reason("No userbase stream with that Id"))))
              case (o, n) =>
                val newStream = replaceOrError(store.get(o.id.key).get, newEvent)
                if (newStream.isRight) {
                  store += (o.id.key -> newStream.toEither.right.toOption.get)
                }
                newStream.map(_ => newEvent)
            }
          }
        }
    }

  /**
   * Replaces an event in a list of events based on the sequence number.
   *
   * @param events The list of events to go through
   * @param newEvent The event to sub in
   * @return The final list of events
   */
  private def replaceOrError[KK, S: Equal, E](events: EventStream[KK, S, E], newEvent: Event[KK, S, E]): \/[EventStreamError, EventStream[KK, S, E]] =
    events.span(_.id.seq /== newEvent.id.seq) match {
      case (a, Nil) => -\/(EventStreamError.reject(NonEmptyList(Reason("Seq doesn't exist in userbase"))))
      case (a, b)   => \/-(a ++ (newEvent :: b.tail))
    }
}
