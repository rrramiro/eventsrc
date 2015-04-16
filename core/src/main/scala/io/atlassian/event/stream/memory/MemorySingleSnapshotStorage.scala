package io.atlassian.event
package stream
package memory

import scalaz.{\/, Monad}
import scalaz.syntax.either._

/**
 * Basic implementation of SnapshotStorage that stores a single (latest) snapshot in memory.
 */
class MemorySingleSnapshotStorage[F[_], K, S, V](implicit M: Monad[F]) extends SnapshotStorage[F, K, S, V] {
  private val map = collection.concurrent.TrieMap[K, Snapshot[K, S, V]]()

  override def get(key: K, sequence: SequenceQuery[S]): F[Snapshot[K, S, V]] =
    M.point {
      sequence.fold(
      { _ => Snapshot.zero},
      Snapshot.zero,
      map.getOrElse(key, Snapshot.zero))
    }

  override def put(snapshotKey: K, snapshot: Snapshot[K, S, V], mode: SnapshotStoreMode): F[SnapshotStorage.Error \/ Snapshot[K, S, V]] =
    M.point {
      map += (snapshotKey -> snapshot)
      snapshot.right
    }
}