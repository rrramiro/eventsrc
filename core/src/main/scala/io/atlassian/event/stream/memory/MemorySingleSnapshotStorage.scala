package io.atlassian.event
package stream
package memory

import scalaz.concurrent.Task
import scalaz.syntax.either._
import scalaz.{ Applicative, \/ }

/**
 * Basic implementation of SnapshotStorage that stores a single (latest) snapshot in memory.
 */
object MemorySingleSnapshotStorage {
  def apply[K, S, V] = Task.delay {
    val map = collection.concurrent.TrieMap[K, Snapshot[S, V]]()

    new SnapshotStorage[Task, K, S, V] {
      override def get(key: K, sequence: SequenceQuery[S]): Task[Snapshot[S, V]] =
        Task.delay {
          sequence.fold(
            { _ => Snapshot.zero },
            Snapshot.zero,
            map.getOrElse(key, Snapshot.zero)
          )
        }

      override def put(snapshotKey: K, snapshot: Snapshot[S, V], mode: SnapshotStoreMode): Task[SnapshotStorage.Error \/ Snapshot[S, V]] =
        Task.delay {
          map += (snapshotKey -> snapshot)
          snapshot.right
        }
    }
  }
}
