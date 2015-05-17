package io.atlassian.event
package stream

import kadai.Invalid

import scalaz.{ Applicative, \/ }
import scalaz.syntax.either._
import scalaz.syntax.applicative._

/**
 * Implementations of this interface deal with persisting snapshots so that they don't need to be recomputed every time.
 * Specifically, implementations do NOT deal with generating snapshots, only storing/retrieving any persisted snapshot.
 * @tparam F Container around operations on an underlying data store e.g. Task.
 * @tparam K The type of the key for snapshots. This does not need to be the same as for the event stream itself.
 * @tparam V The type of the value wrapped by Snapshots that this store persists.
 */
trait SnapshotStorage[F[_], K, S, V] {
  /**
   * Retrieve a snapshot before the given sequence number. We typically specify a sequence number if we want to get
   * some old snapshot i.e. the latest persisted snapshot may have been generated after the point in time that we're
   * interested in.
   *
   * @param key The key
   * @param sequence What sequence we want to get the snapshot for (earliest snapshot, latest, or latest before some sequence)
   * @return The snapshot, a NoSnapshot if there was no snapshot for the given conditions.
   */
  def get(key: K, sequence: SequenceQuery[S]): F[Snapshot[S, V]]

  /**
   * Save a given snapshot
   * @param snapshotKey The key
   * @param snapshot The snapshot to save
   * @param mode Defines whether the given snapshot should be deemed the earliest point in the event stream (Epoch) or not (Cache)
   * @return Either a Throwable (for error) or the saved snapshot.
   */
  def put(snapshotKey: K, snapshot: Snapshot[S, V], mode: SnapshotStoreMode): F[SnapshotStorage.Error \/ Snapshot[S, V]]
}

/**
 * Dummy implementation that does not store snapshots.
 */
object SnapshotStorage {
  def none[F[_]: Applicative, K, S, V]: SnapshotStorage[F, K, S, V] =
    new NoSnapshotStorage

  private class NoSnapshotStorage[F[_]: Applicative, K, S, V] extends SnapshotStorage[F, K, S, V] {
    def get(key: K, sequence: SequenceQuery[S]): F[Snapshot[S, V]] =
      Snapshot.zero[S, V].point[F]

    def put(key: K, snapshot: Snapshot[S, V], mode: SnapshotStoreMode): F[Error \/ Snapshot[S, V]] =
      snapshot.right[Error].point[F]
  }

  sealed trait Error
  object Error {
    def unknown(i: Invalid): Error =
      Unknown(i)

    case class Unknown(i: Invalid) extends Error
  }
}

sealed trait SnapshotStoreMode {
  import SnapshotStoreMode._

  def fold[X](epoch: => X, cache: => X): X =
    this match {
      case Epoch => epoch
      case Cache => cache
    }
}

object SnapshotStoreMode {
  case object Epoch extends SnapshotStoreMode
  case object Cache extends SnapshotStoreMode
}