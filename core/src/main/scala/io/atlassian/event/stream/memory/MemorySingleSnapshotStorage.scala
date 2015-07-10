package io.atlassian.event
package stream
package memory

import scalaz.{ Applicative, \/ }
import scalaz.syntax.either._

/**
 * Basic implementation of SnapshotStorage that stores a single (latest) snapshot in memory.
 */
object MemorySingleSnapshotStorage {
  def apply[F[_], K, S, V](implicit A: Applicative[F]): SnapshotStorage[F, K, S, V] =
    new MemorySingleSnapshotStorage[F, K, S, V]()
}

class MemorySingleSnapshotStorage[F[_], K, S, V](implicit A: Applicative[F]) extends SnapshotStorage[F, K, S, V] {
  private val map = collection.concurrent.TrieMap[K, Snapshot[S, V]]()

  override def get(key: K, sequence: SequenceQuery[S]): F[Snapshot[S, V]] =
    A.point {
      sequence.fold(
        { _ => Snapshot.zero },
        Snapshot.zero,
        map.getOrElse(key, Snapshot.zero)
      )
    }

  override def put(snapshotKey: K, snapshot: Snapshot[S, V], mode: SnapshotStoreMode): F[SnapshotStorage.Error \/ Snapshot[S, V]] =
    A.point {
      map += (snapshotKey -> snapshot)
      snapshot.right
    }
}