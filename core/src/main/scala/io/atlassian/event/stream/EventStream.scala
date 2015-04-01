package io.atlassian.event
package stream

import org.joda.time.DateTime

import scalaz._
import scalaz.stream.{ process1, Process }
import scalaz.syntax.either._
import scalaz.syntax.monad._

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
   * This is the main interface for consumers of the Event source. Implementations of this wrap logic for generating
   * a view of data or aggregate (wrapped in a Snapshot) from an event stream of E events.
   *
   * Upon construction of an API, a suitable Events store, snapshot store, accumulator, and key transform
   * need to be provided.
   *
   * @tparam Key type for the aggregate (may be different from the key for the event stream)
   * @tparam Val Aggregate type
   */
  trait API[Key, Val] {

    type K = Key
    type V = Val

    private[stream] def snapshotStore: SnapshotStorage[F, K, S, V]

    /**
     * Accumulator function for generating a view of the data from a previous view and a new event.
     * @param v The previous view
     * @param e Event to add to previous view
     * @return New view with event added.
     */
    protected def acc(key: K)(v: Snapshot[K, S, V], e: Event[KK, S, E]): Snapshot[K, S, V]

    /**
     * Transform a given aggregate key to the key for the underlying event stream
     */
    protected def eventStreamKey: K => KK

    /**
     * Return the current view of the data for key 'key'
     */
    final def get(key: K): F[Option[V]] =
      getSnapshot(key).map { _.value }

    /**
     * @return the current view wrapped in Snashot of the data for key 'key'
     */
    final def getSnapshot(key: K): F[Snapshot[K, S, V]] =
      getSnapshotAt(key, None)

    /**
     * @param key The key of the aggregate to retrieve
     * @param at If none, get me the latest. If some, get me the snapshot at that specific sequence.
     * @return A Snapshot for the aggregate at the given sequence number.
     */
    private def getSnapshotAt(key: K, at: Option[S]): F[Snapshot[K, S, V]] =
      for {
        persistedSnapshot <- snapshotStore.get(key, at.fold(SequenceQuery.latest[S])(SequenceQuery.before))
        fromSeq = persistedSnapshot.seq
        pred = at.fold[Event[KK, S, E] => Boolean] { _ => true } { seq => e => S.order.lessThanOrEqual(e.id.seq, seq) }
        events = eventStore.get(eventStreamKey(key), fromSeq).takeWhile(pred)
        theSnapshot <- generateSnapshot(persistedSnapshot, events, acc(key))
      } yield theSnapshot

    /**
     * Return the view of the data for the key 'key' at the specified sequence number.
     * @param key the key
     * @param seq the sequence number of the event at which we want the see the view of the data.
     * @return view of the data at event with sequence 'seq'
     */
    final def getAt(key: K, seq: S): F[Option[V]] =
      getSnapshotAt(key, Some(seq)).map { _.value }

    /**
     * Get a stream of Snapshots starting from sequence number 'from' (if defined).
     * @param key The key
     * @param from Starting sequence number. None to get from the beginning of the stream.
     * @return a stream of Snapshots starting from sequence number 'from' (if defined).
     */
    final def getHistory(key: K, from: Option[S]): F[Process[F, Snapshot[K, S, V]]] =
      for {
        startingSnapshot <- getSnapshotAt(key, from)
      } yield eventStore.get(eventStreamKey(key), startingSnapshot.seq)
        .scan[Snapshot[K, S, V]](startingSnapshot) {
          case (view, event) => acc(key)(view, event)
        }.drop(1)

    /**
     * Return the view of the data for the key 'key' at the specified timestamp.
     *
     * In this case we don't know the starting sequence number, so just start from the first available
     * @param key The key
     * @param time The timestamp at which we want to see the view of the data
     * @return view of the data with events up to the given time stamp.
     */
    final def getAt(key: K, time: DateTime): F[Option[V]] = {
      import com.github.nscala_time.time.Implicits._
      // We need to get the earliest snapshot, then the stream of events from that snapshot
      for {
        earliestSnapshot <- snapshotStore.get(key, SequenceQuery.earliest[S])
        value <- generateSnapshot(earliestSnapshot, eventStore.get(eventStreamKey(key), earliestSnapshot.seq).takeWhile { _.time <= time }, acc(key)).map { _.value }
      } yield value
    }

    /**
     * Save an event (generated from the given Operation) for the given key.
     *
     * Pass in the function that creates a possible transform,
     * given the current Snapshot if there is one.
     */
    /*
     * To save a new value, we need to get the latest snapshot in order to get the existing view of data and the
     * latest event Id. Then we create a suitable transform and event and try to save it. Upon duplicate event,
     * try the operation again (highly unlikely that this situation would occur).
     *
     * TODO - Consider adding some jitter and exponential backoff
     */
    final def save(key: K, operation: Operation[K, S, V, E]): F[SaveResult[K, S, V]] =
      for {
        old <- getSnapshot(key)
        op = operation.run(old)
        result <- op.fold(
          Error.noop.left[Event[KK, S, E]].point[F],
          Error.reject(_).left[Event[KK, S, E]].point[F],
          e => eventStore.put(Event.next[KK, S, E](eventStreamKey(key), old.seq, e))
        )
        transform <- result match {
          case -\/(Error.DuplicateEvent) =>
            save(key, operation)
          case -\/(Error.Noop) =>
            SaveResult.noop[K, S, V]().point[F]
          case -\/(Error.Rejected(r)) =>
            SaveResult.reject[K, S, V](r).point[F]
          case \/-(event) =>
            SaveResult.success[K, S, V](acc(key)(old, event)).point[F]
        }
      } yield transform

    /**
     * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
     * @param events The stream of events.
     * @return Container F that when executed provides the snapshot.
     */
    private def generateSnapshot(start: Snapshot[K, S, V], events: Process[F, Event[KK, S, E]], f: (Snapshot[K, S, V], Event[KK, S, E]) => Snapshot[K, S, V]): F[Snapshot[K, S, V]] =
      events.pipe {
        process1.fold(start)(f)
      }.runLastOr(start)

    /**
     * Updates stored snapshot that this API wraps.
     * TODO - How should we best expose this functionality. We need to be able to update stored snapshots asynchronously
     * either lazily (i.e. on read) or eagerly (i.e. when we save a new event).
     * @param key The key
     * @param forceStartAt If present, this will regenerate a snapshot starting from events at the specified sequence number.
     *                     This should only be used when it is known that preceding events can be ignored. For example
     *                     when new entities are added, there are no views of those entities before the events that add
     *                     them!
     * @return Error when saving snapshot or the snapshot that was saved.
     */
    final def refreshSnapshot(key: K, forceStartAt: Option[S]): F[SnapshotStorage.Error \/ Snapshot[K, S, V]] =
      forceStartAt match {
        case None =>
          for {
            latestStored <- snapshotStore.get(key, SequenceQuery.latest)
            events = eventStore.get(eventStreamKey(key), latestStored.seq)
            latestSnapshot <- generateSnapshot(latestStored, events, acc(key))
            saveResult <-
              if (latestSnapshot.seq != latestStored.seq)
                saveSnapshot(key, latestSnapshot)
              else
                latestSnapshot.right[SnapshotStorage.Error].point[F]
          } yield saveResult

        case start @ Some(s) =>
          for {
            snapshotToSave <- generateSnapshot(Snapshot.zero, eventStore.get(eventStreamKey(key), start), acc(key))
            saveResult <- saveSnapshot(key, snapshotToSave)
          } yield saveResult
      }

    /**
     * Forces a save of the given snapshot. This can be used as a shortcut to refreshSnapshot if you already have the
     * snapshot that you want to save.
     * TODO - How should we best expose this functionality. Feels like a bit of a hack for optimisation.
     * @param key The key
     * @param snapshot Snapshot to save
     * @return Error when saving snapshot or the snapshot that was saved.
     */
    final def saveSnapshot(key: K, snapshot: Snapshot[K, S, V]): F[SnapshotStorage.Error \/ Snapshot[K, S, V]] =
      snapshotStore.put(key, snapshot)

  }
}