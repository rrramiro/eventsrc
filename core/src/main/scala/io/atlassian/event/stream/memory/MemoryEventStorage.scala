package io.atlassian.event.stream.memory

import io.atlassian.event.Sequence
import io.atlassian.event.stream.{ Event, EventStorage, EventStreamError }

import scalaz.{ OptionT, Traverse, \/ }
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.order._
import scalaz.syntax.traverse._

/**
 * Basic implementation that stores events in an in-memory map.
 */
object MemoryEventStorage {
  def apply[KK, S: Sequence, E] = Task.delay {
    val map = collection.concurrent.TrieMap[KK, List[Event[KK, S, E]]]()

    new EventStorage[Task, KK, S, E] {
      override def get(key: KK, fromOption: Option[S]): Process[Task, Event[KK, S, E]] = {
        Process.await(Task.delay {
          map.get(key)
        }) {
          case None => Process.halt
          case Some(cs) =>
            Process.emitAll(cs.reverse.dropWhile { ev => fromOption.fold(false) { from => ev.id.seq <= from } })
        }
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

      def latest(key: KK) =
        OptionT(Task.delay {
          map.getOrElse(key, Nil).headOption
        })
    }
  }
}
