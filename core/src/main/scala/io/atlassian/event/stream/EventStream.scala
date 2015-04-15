package io.atlassian.event
package stream

import org.joda.time.DateTime

import scalaz._
import scalaz.stream.{ process1, Process }
import scalaz.syntax.all._
import scalaz.syntax.std.option._

object EventStream {
  /**
   * EventStream.Error represents any error conditions that are useful to represent for event sources. In particular,
   * we need to know about attempts to store duplicate events.
   */
  sealed trait Error
  object Error {
    def noop: Error = Noop

    def reject(s: NonEmptyList[Reason]): Error = Rejected(s)

    case object DuplicateEvent extends Error

    case class Rejected(s: NonEmptyList[Reason]) extends Error

    // if the client rejects an update operation
    case object Noop extends Error
  }

  sealed trait QueryConsistency {
    import QueryConsistency._

    def fold[X](snapshot: => X, event: => X): X =
      this match {
        case LatestSnapshot => snapshot
        case LatestEvent => event
      }
  }
  object QueryConsistency {
    case object LatestSnapshot extends QueryConsistency
    case object LatestEvent extends QueryConsistency

    val latestSnapshot: QueryConsistency = LatestSnapshot
    val latestEvent: QueryConsistency = LatestEvent
  }
}

/**
 * The main trait representing a stream of events. It wraps up the types required, and provides basic
 * interfaces that need to be implemented to expose an API for consumers, and also to store events and any caches of
 * views.
 *
 * @tparam F Container type for API operations. It needs to be a Monad and a Catchable (e.g. scalaz Task)
 */
abstract class EventStream[F[_]: Monad: Catchable] {
  import EventStream._

  // The internal key against which values are stored.
  type KK

  // Type of the sequence. Needs to have a Sequence type class implementation.
  type S

  // Event payload that the stream deals with.
  type E

  protected implicit def S: Sequence[S]

  protected def eventStore: EventStorage[F, KK, S, E]

  /**
   * QueryAPI provides the main API for consumers to query an event source. It is a basic key-value store.
   * Consumers can query by a `Key` type that needs to be transformable to the underlying event stream's `K` key type.
   * Implementations contain logic for calculating a snapshot given a series of events, key transform, and
   * override `runPersistSnapshot` to control how snapshots are persisted.
   * 
   * @tparam Key type for the aggregate (may be different from the key for the event stream)
   * @tparam Val Aggregate type
   */
  trait QueryAPI[Key, Val] {
    type K = Key
    type V = Val

    private[stream] def snapshotStore: SnapshotStorage[F, K, S, V]

    /**
     * Accumulator function for generating a view of the data from a previous view and a new event.
     * @param v The previous view
     * @param e Event to add to previous view
     * @return New view with event added.
     */
    private[stream] def acc(key: K)(v: Snapshot[K, S, V], e: Event[KK, S, E]): Snapshot[K, S, V]

    /**
     * Transform a given aggregate key to the key for the underlying event stream
     */
    private[stream] def eventStreamKey: K => KK

    /**
     * Wraps output from `generateLatestSnapshot` which is both the latest snapshot and what was previously persisted.
     */
    protected case class LatestSnapshotResult(latest: Snapshot[K, S, V], previousPersisted: Snapshot[K, S, V])
    
    /**
     * Return the current view of the data for key 'key'
     */
    final def get(key: K, consistency: QueryConsistency = QueryConsistency.LatestEvent): F[Option[V]] =
      getSnapshot(key, consistency).map { _.value }

    /**
     * @return the current view wrapped in Snapshot of the data for key 'key'
     */
    final def getSnapshot(key: K, consistency: QueryConsistency = QueryConsistency.LatestEvent): F[Snapshot[K, S, V]] =
      consistency.fold(
        snapshotStore.get(key, SequenceQuery.latest[S]),
        for {
          latestSnapshot <- generateLatestSnapshot(key)
          _ = persistSnapshot(key, latestSnapshot.latest, latestSnapshot.previousPersisted.some)
        } yield latestSnapshot.latest
      )

    /**
     * Generates the latest snapshot by retrieving the last persisted snapshot and then replaying events on top of that.
     */
    private[stream] final def generateLatestSnapshot(key: K): F[LatestSnapshotResult] =
      for {
        persistedSnapshot <- snapshotStore.get(key, SequenceQuery.latest[S])
        fromSeq = persistedSnapshot.seq
        events = eventStore.get(eventStreamKey(key), fromSeq)
        theSnapshot <- snapshotFold(persistedSnapshot, events, acc(key))
      } yield LatestSnapshotResult(theSnapshot, persistedSnapshot)

    /**
     * @param key The key of the aggregate to retrieve
     * @param at If none, get me the latest. If some, get me the snapshot at that specific sequence.
     * @return A Snapshot for the aggregate at the given sequence number.
     */
    private def generateSnapshotAt(key: K, at: Option[S]): F[Snapshot[K, S, V]] =
      for {
        persistedSnapshot <- snapshotStore.get(key, at.fold(SequenceQuery.latest[S])(SequenceQuery.before))
        fromSeq = persistedSnapshot.seq
        pred = at.fold[Event[KK, S, E] => Boolean] { _ => true } { seq => e => S.order.lessThanOrEqual(e.id.seq, seq) }
        events = eventStore.get(eventStreamKey(key), fromSeq).takeWhile(pred)
        theSnapshot <- snapshotFold(persistedSnapshot, events, acc(key))
      } yield theSnapshot

    /**
     * Return the view of the data for the key 'key' at the specified sequence number.
     * @param key the key
     * @param seq the sequence number of the event at which we want the see the view of the data.
     * @return view of the data at event with sequence 'seq'
     */
    final def getAt(key: K, seq: S): F[Option[V]] =
      generateSnapshotAt(key, Some(seq)).map { _.value }

    /**
     * Get a stream of Snapshots starting from sequence number 'from' (if defined).
     * @param key The key
     * @param from Starting sequence number. None to get from the beginning of the stream.
     * @return a stream of Snapshots starting from sequence number 'from' (if defined).
     */
    final def getHistory(key: K, from: Option[S]): F[Process[F, Snapshot[K, S, V]]] =
      for {
        startingSnapshot <- generateSnapshotAt(key, from)
      } yield eventStore.get(eventStreamKey(key), startingSnapshot.seq)
        .scan[Snapshot[K, S, V]](startingSnapshot) {
        case (view, event) => acc(key)(view, event)
      }.drop(1)

    /**
     * Return the view of the data for the key 'key' at the specified timestamp.
     *
     * @param key The key
     * @param time The timestamp at which we want to see the view of the data
     * @return view of the data with events up to the given time stamp.
     */
    final def getAt(key: K, time: DateTime): F[Option[V]] = {
      import com.github.nscala_time.time.Implicits._
      // We need to get the earliest snapshot, then the stream of events from that snapshot
      for {
        earliestSnapshot <- snapshotStore.get(key, SequenceQuery.earliest[S])
        value <- snapshotFold(earliestSnapshot, eventStore.get(eventStreamKey(key), earliestSnapshot.seq).takeWhile { _.time <= time }, acc(key)).map { _.value }
      } yield value
    }

    /**
     * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
     * @param events The stream of events.
     * @return Container F that when executed provides the snapshot.
     */
    private def snapshotFold(start: Snapshot[K, S, V],
                             events: Process[F, Event[KK, S, E]],
                             f: (Snapshot[K, S, V], Event[KK, S, E]) => Snapshot[K, S, V]): F[Snapshot[K, S, V]] =
      events.pipe {
        process1.fold(start)(f)
      }.runLastOr(start)

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
    final def forceRefreshPersistedSnapshot(key: K, forceStartAt: S): F[SnapshotStorage.Error \/ Snapshot[K, S, V]] =
      for {
        snapshotToSave <- snapshotFold(Snapshot.zero, eventStore.get(eventStreamKey(key), Some(forceStartAt)), acc(key))
        saveResult <- persistSnapshotOp(key, snapshotToSave, None)
      } yield saveResult

    private final def persistSnapshotOp(key: K, snapshot: Snapshot[K, S, V], previousSnapshot: Option[Snapshot[K, S, V]]): F[SnapshotStorage.Error \/ Snapshot[K, S, V]] =
      if (snapshot.seq != previousSnapshot.map { _.seq })
        snapshotStore.put(key, snapshot, SnapshotStoreMode.Cache)
      else
        snapshot.right[SnapshotStorage.Error].point[F]

    /**
     * Override this to control how snapshot persistence happens (e.g. asynchronously, synchronously)
     */
    protected def runPersistSnapshot: F[SnapshotStorage.Error \/ Snapshot[K, S, V]] => Unit =
      _ => ()

    /**
     * Save the given `snapshot` if it is at a different sequence number to `previousSnapshot`. Set `previousSnapshot`
     * to None to force a save.
     */
    private[stream] def persistSnapshot(key: K, snapshot: Snapshot[K, S, V], previousSnapshot: Option[Snapshot[K, S, V]]): Unit =
      persistSnapshotOp(key, snapshot, previousSnapshot) |> runPersistSnapshot
  }

  /**
   * SaveAPI allows consumers to save to the event stream. The aggregator argument i.e. QueryAPI implementation provides
   * the logic for generating a suitable aggregator/snapshot on which to apply any logic encapsulated by the given
   * `save` function `operation` argument.
   *
   * @tparam Key type for the aggregate (may be different from the key for the event stream)
   * @tparam Val Aggregate type
   */
  class SaveAPI[Key, Val](aggregator: QueryAPI[Key, Val]) {
    type K = Key
    type V = Val

    import aggregator._

    final def save(key: K, operation: Operation[K, S, V, E]): F[SaveResult[K, S, V]] =
      for {
        old <- generateLatestSnapshot(key)
        op = operation.run(old.latest)
        result <- op.fold(
          Error.noop.left[Event[KK, S, E]].point[F],
          Error.reject(_).left[Event[KK, S, E]].point[F],
          e => eventStore.put(Event.next[KK, S, E](eventStreamKey(key), old.latest.seq, e))
        )
        transform <- result match {
          case -\/(Error.DuplicateEvent) =>
            save(key, operation)
          case -\/(Error.Noop) =>
            SaveResult.noop[K, S, V]().point[F]
          case -\/(Error.Rejected(r)) =>
            SaveResult.reject[K, S, V](r).point[F]
          case \/-(event) =>
            val newSnapshot = acc(key)(old.latest, event)

            for {
              saveResult <- SaveResult.success[K, S, V](newSnapshot).point[F]
              _ = persistSnapshot(key, newSnapshot, old.previousPersisted.some)
            } yield saveResult
        }
      } yield transform
  }
}