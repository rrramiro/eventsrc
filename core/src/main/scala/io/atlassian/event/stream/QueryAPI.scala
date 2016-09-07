package io.atlassian.event
package stream

import org.joda.time.DateTime

import scalaz.{ Catchable, Contravariant, Monad, \/ }
import scalaz.stream.{ Process, process1 }
import scalaz.syntax.all._
import scalaz.syntax.std.option._

case class LatestSnapshotResult[S, V](latest: Snapshot[S, V], previousPersisted: Snapshot[S, V])

sealed trait QueryAPI[F[_], K, S, E, V] {
  /**
   * @return the current view of the data for key 'key'
   */
  def get(key: K, consistency: QueryConsistency): F[Option[V]]

  /**
   * @return the current view wrapped in Snapshot of the data for key 'key'
   */
  def getSnapshot(key: K, consistency: QueryConsistency): F[Snapshot[S, V]]

  /**
   * Generates the latest snapshot by retrieving the last persisted snapshot and then replaying events on top of that.
   */
  def generateLatestSnapshot(key: K): F[LatestSnapshotResult[S, V]]

  /**
   * Return the view of the data for the key 'key' at the specified sequence number.
   * @param key the key
   * @param seq the sequence number of the event at which we want the see the view of the data.
   * @return view of the data at event with sequence 'seq'
   */
  def getAt(key: K, seq: S): F[Option[V]]

  /**
   * Get a stream of Snapshots starting from sequence number 'from' (if defined).
   * @param key The key
   * @param from Starting sequence number. None to get from the beginning of the stream.
   * @return a stream of Snapshots starting from sequence number 'from' (if defined).
   */
  def getHistory(key: K, from: Option[S]): F[Process[F, Snapshot[S, V]]]

  /**
   * Return the view of the data for the key 'key' at the specified timestamp.
   *
   * @param key The key
   * @param time The timestamp at which we want to see the view of the data
   * @return view of the data with events up to the given time stamp.
   */
  def getAt(key: K, time: DateTime): F[Option[V]]

  /**
   * Explicitly refresh persisted snapshot with events starting at `forceStartAt`. Normally to refresh a snapshot,
   * your implementation of QueryAPI can do so asynchronously via a custom `onGenerateLatestSnapshot` function.
   *
   * WARNING - Use this only if you know that events prior to `forceStartAt` can be safely ignored. Typically this is
   * when a single event stream contains events for multiple entities, so obviously when you create a new entity, you
   * can ignore all events prior to that creation event.
   *
   * @param key The key
   * @param forceStartAt Generate a snapshot starting from events at the specified sequence number.
   *                     This should only be used when it is known that preceding events can be ignored. For example
   *                     when new entities are added, there are no views of those entities before the events that add
   *                     them!
   * @return Error when saving snapshot or the snapshot that was saved.
   */
  def forceRefreshPersistedSnapshot(key: K, forceStartAt: S): F[SnapshotStorage.Error \/ Snapshot[S, V]]

  /**
   * contramap on the key.
   */
  def contramap[KK](f: KK => K): QueryAPI[F, KK, S, E, V]
}

object QueryAPI {

  def apply[F[_], KK, E, K, S, V](
    toStreamKey: K => KK,
    eventStore: EventStorage[F, KK, S, E],
    snapshotStore: SnapshotStorage[F, K, S, V],
    acc: K => (Snapshot[S, V], Event[KK, S, E]) => Snapshot[S, V]
  )(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]): QueryAPI[F, K, S, E, V] =
    new Impl(toStreamKey, eventStore, snapshotStore, acc)

  private[QueryAPI] class Impl[F[_], K, S, E, V, KK](
      toStreamKey: K => KK,
      eventStore: EventStorage[F, KK, S, E],
      snapshotStore: SnapshotStorage[F, K, S, V],
      acc: K => (Snapshot[S, V], Event[KK, S, E]) => Snapshot[S, V]
  )(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]) extends QueryAPI[F, K, S, E, V] {

    override def get(key: K, consistency: QueryConsistency): F[Option[V]] =
      getSnapshot(key, consistency).map { _.value }

    override def getSnapshot(key: K, consistency: QueryConsistency): F[Snapshot[S, V]] =
      consistency.fold(
        snapshotStore.get(key, SequenceQuery.latest[S]),
        for {
          latestSnapshot <- generateLatestSnapshot(key)
          _ <- persistSnapshot(key, latestSnapshot.latest, latestSnapshot.previousPersisted.some)
        } yield latestSnapshot.latest
      )

    override def generateLatestSnapshot(key: K): F[LatestSnapshotResult[S, V]] =
      for {
        persistedSnapshot <- snapshotStore.get(key, SequenceQuery.latest[S])
        fromSeq = persistedSnapshot.seq
        events = eventStore.get(toStreamKey(key), fromSeq)
        theSnapshot <- snapshotFold(persistedSnapshot, events, acc(key))
      } yield LatestSnapshotResult(theSnapshot, persistedSnapshot)

    override def getAt(key: K, seq: S): F[Option[V]] =
      generateSnapshotAt(key, Some(seq)).map { _.value }

    override def getHistory(key: K, from: Option[S]): F[Process[F, Snapshot[S, V]]] =
      generateSnapshotAt(key, from).map { start =>
        eventStore.get(toStreamKey(key), start.seq).scan(start) { acc(key) }.drop(1)
      }

    override def getAt(key: K, time: DateTime): F[Option[V]] = {
      import com.github.nscala_time.time.Implicits._
      // We need to get the earliest snapshot, then the stream of events from that snapshot
      for {
        earliestSnapshot <- snapshotStore.get(key, SequenceQuery.earliest[S])
        value <- snapshotFold(earliestSnapshot, eventStore.get(toStreamKey(key), earliestSnapshot.seq).takeWhile { _.time <= time }, acc(key)).map { _.value }
      } yield value
    }

    override def forceRefreshPersistedSnapshot(key: K, forceStartAt: S): F[SnapshotStorage.Error \/ Snapshot[S, V]] =
      snapshotFold(Snapshot.zero, eventStore.get(toStreamKey(key), Some(forceStartAt)), acc(key)) >>= { persistSnapshot(key, _, None) }

    def persistSnapshot(key: K, snapshot: Snapshot[S, V], previousSnapshot: Option[Snapshot[S, V]]): F[SnapshotStorage.Error \/ Snapshot[S, V]] =
      if (snapshot.seq != previousSnapshot.map { _.seq })
        snapshotStore.put(key, snapshot, SnapshotStoreMode.Cache)
      else
        snapshot.right[SnapshotStorage.Error].point[F]

    override def contramap[A](f: A => K): QueryAPI[F, A, S, E, V] =
      new Impl[F, A, S, E, V, KK](
        f andThen toStreamKey,
        eventStore,
        snapshotStore.contramap(f),
        f andThen acc
      )

    private def generateSnapshotAt(key: K, at: Option[S]): F[Snapshot[S, V]] =
      for {
        persistedSnapshot <- snapshotStore.get(key, at.fold(SequenceQuery.latest[S])(SequenceQuery.before))
        fromSeq = persistedSnapshot.seq
        pred = at.fold[Event[KK, S, E] => Boolean] { _ => true } { seq => e => e.id.seq <= seq }
        events = eventStore.get(toStreamKey(key), fromSeq).takeWhile(pred)
        theSnapshot <- snapshotFold(persistedSnapshot, events, acc(key))
      } yield theSnapshot

    /**
     * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
     * @param events The stream of events.
     * @return Container F that when executed provides the snapshot.
     */
    private def snapshotFold(
      start: Snapshot[S, V],
      events: Process[F, Event[KK, S, E]],
      f: (Snapshot[S, V], Event[KK, S, E]) => Snapshot[S, V]
    ): F[Snapshot[S, V]] =
      events.pipe { process1.fold(start)(f) }.runLastOr(start)
  }
}
