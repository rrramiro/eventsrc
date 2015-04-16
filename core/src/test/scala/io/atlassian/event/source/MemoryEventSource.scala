package io.atlassian.event
package source

import scalaz.{ Catchable, Monad, \/ }
import scalaz.std.option._
import scalaz.syntax.either._
import scalaz.syntax.std.option._
import scalaz.concurrent.Task
import scalaz.stream.Process

/**
 * Simple event store that keeps lists of commits for a key in a mutable map
 */
class MemoryEventSource extends LongSequencedEventSource[Int, String] {
  import EventSource.Error

  val map = collection.concurrent.TrieMap[Int, List[Event]]()

  object api extends API[Task] {
    val M = Monad[Task]
    val C = Catchable[Task]

    object store extends Storage[Task] {
      override def get(key: Int, seq: Option[Long]): Process[Task, Event] =
        map.get(key) match {
          case None     => Process.halt
          case Some(cs) => Process.emitAll(cs.reverse.takeWhile { ev => seq.fold(true) { _ >= ev.id.sequence } })
        }

      override def put(ev: Event): Task[Error \/ Event] =
        Task {
          // Just assume there are no duplicates for this test
          map += (ev.id.key -> (ev :: map.getOrElse(ev.id.key, Nil)))
          ev.right
        }
    }
  }
}