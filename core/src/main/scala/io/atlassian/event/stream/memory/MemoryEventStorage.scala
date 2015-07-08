package io.atlassian.event.stream.memory

import io.atlassian.event.Sequence
import io.atlassian.event.stream.{ Event, EventStorage, EventStreamError }

import scalaz.\/
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.order._

/**
 * Basic implementation that stores events in an in-memory map.
 */
class MemoryEventStorage[KK, S: Sequence, E] extends EventStorage[Task, KK, S, E] {
  private[this] val map = collection.concurrent.TrieMap[KK, List[Event[KK, S, E]]]()

  override def get(key: KK, fromOption: Option[S]): Process[Task, Event[KK, S, E]] =
    map.get(key) match {
      case None => Process.halt
      case Some(cs) =>
        Process.emitAll(cs.reverse.dropWhile { ev => fromOption.fold(false) { from => ev.id.seq <= from } })
    }

  override def put(ev: Event[KK, S, E]): Task[EventStreamError \/ Event[KK, S, E]] =
    Task.delay {
      map.synchronized {
        val currentList = map.getOrElse(ev.id.key, Nil)
        currentList match {
          case h :: hs if h.id.seq >= ev.id.seq =>
            EventStreamError.DuplicateEvent.left
          case _ =>
            map += (ev.id.key -> (ev :: currentList))
            ev.right
        }
      }
    }
}
