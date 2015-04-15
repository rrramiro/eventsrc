package io.atlassian.event
package stream
package dynamo

import scalaz.{Monad, \/}
import scalaz.syntax.either._

/**
 * Basic implementation of snapshot storage using Dynamo for persistence. It stores only a single snapshot
 * that is overwritten over time.
 */
class DynamoSingleSnapshotStorage[F[_], K, S, V] extends SnapshotStorage[F, K, S, V] {

  override def get(key: K, sequence: SequenceQuery[S]): F[Snapshot[K, S, V]] = ???
  override def put(snapshotKey: K, snapshot: Snapshot[K, S, V], mode: SnapshotStoreMode): F[SnapshotStorage.Error \/ Snapshot[K, S, V]] = ???
}

