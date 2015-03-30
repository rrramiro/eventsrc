package io.atlassian.eventsrc

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
 * TODO - Should this replace EventSource, or should we make EventSource implement this?
 *
 * @tparam K The key against which values are stored.
 * @tparam E Event payload that the stream deals with.
 * @tparam S Type of the sequence. Needs to have a Sequence type class implementation.
 */
trait EventStream {
  import EventStream._

  type K
  type S
  type E

  def S: Sequence[S]

  /**
   * A event is identified by the key and an incrementing sequence 'number'
   * @param key The key
   * @param sequence the sequence number
   */
  case class EventId(key: K, sequence: S)

  object EventId {
    def first(key: K): EventId =
      EventId(key, S.first)

    def next(id: EventId): EventId =
      id.copy(sequence = S.next(id.sequence))
  }

  /**
   * Event wraps the event payload with common information (event id and time of the event)
   */
  case class Event(id: EventId, time: DateTime, operation: E)

  object Event {
    def next[A](key: K, snapshot: Snapshot[A], op: E): Event =
      Event(snapshot.id.map { EventId.next }.getOrElse { EventId.first(key) }, DateTime.now, op)
  }

  /**
   * A Snapshot wraps an optional value, and tags it with an event Id. We can say a 'snapshot' S of key K at event
   * S.at is value S.value. The value is somehow generated from the event stream (see API.acc)
   *
   * The event Id is quite a useful thing in addition to the value of the snapshot.
   *
   * @tparam A The type of the value wrapped by the Snapshot
   */
  sealed trait Snapshot[A] {
    import Snapshot._
    def value: Option[A]
    def id: Option[EventId] =
      this.fold(None, { case (_, at, _) => Some(at) }, { case (at, _) => Some(at) })

    def fold[X](none: => X, value: (A, EventId, DateTime) => X, deleted: (EventId, DateTime) => X): X =
      this match {
        case NoSnapshot()       => none
        case Value(v, at, time) => value(v, at, time)
        case Deleted(at, time)  => deleted(at, time)
      }
  }

  object Snapshot {
    /**
     * There is no snapshot... i.e. no events have been saved.
     */
    case class NoSnapshot[V]() extends Snapshot[V] {
      val value = None
    }

    /**
     * Events have been saved and there is a value stored.
     * @param view The value
     * @param at Represents the point in the stream that this Snapshot is for
     */
    case class Value[V](view: V, at: EventId, time: DateTime) extends Snapshot[V] {
      val value = Some(view)
    }

    /**
     * Events have been saved and there is no value (i.e. the value has been deleted).
     * @param at Represents the point in the stream
     */
    case class Deleted[V](at: EventId, time: DateTime) extends Snapshot[V] {
      val value = None
    }

    def zero[V]: Snapshot[V] = NoSnapshot[V]()
    def value[V](view: V, at: EventId, time: DateTime): Snapshot[V] =
      Value(view, at, time)
    def deleted[V](at: EventId, time: DateTime): Snapshot[V] =
      Deleted(at, time)
  }

  /**
   * Result of saving an event to the stream.
   * @tparam A The aggregate type.
   */
  sealed trait SaveResult[A]
  object SaveResult {
    // TODO - separate delete and insert/update cases?
    case class Success[A](value: Snapshot[A]) extends SaveResult[A]
    case class Reject[A](reasons: NonEmptyList[Reason]) extends SaveResult[A]
    case class Noop[A]() extends SaveResult[A]

    def success[A](a: Snapshot[A]): SaveResult[A] =
      Success(a)

    def noop[A](): SaveResult[A] =
      Noop[A]()

    def reject[A](reasons: NonEmptyList[Reason]): SaveResult[A] =
      Reject(reasons)
  }

  /**
   * Wraps an operation to save an event to an event stream. Saving to an event stream is through an API, which is tied
   * to an aggregate type (wrapped in a Snapshot).
   * @param run Function from a snapshot to an operation that should occur (i.e. should we save the event or reject it)
   * @tparam V The type of the aggregate.
   */
  case class Operation[V](run: Snapshot[V] => Operation.Result[V])

  object Operation {
    sealed trait Result[A] {
      import Result._

      def orElse(other: => Result[A]): Result[A] =
        this match {
          case s @ Success(_) => s
          case _              => other
        }

      def fold[T](noop: => T, reject: NonEmptyList[Reason] => T, success: E => T): T =
        this match {
          case Success(t) => success(t)
          case Reject(r)  => reject(r)
          case Noop()     => noop
        }
    }

    object Result {
      case class Success[A](event: E) extends Result[A]
      case class Reject[A](reasons: NonEmptyList[Reason]) extends Result[A]
      case class Noop[A]() extends Result[A]

      def success[A](e: E): Result[A] =
        Success(e)

      def reject[A](r: NonEmptyList[Reason]): Result[A] =
        Reject(r)

      def noop[A]: Result[A] =
        Noop()
    }
  }

  /**
   * A source of events. Implementations wrap around an underlying data store (e.g. in-memory map or DynamoDB).
   *
   * @tparam F Container around operations on an underlying data store e.g. Task.
   */
  trait EventStorage[F[_]] {
    /**
     * Retrieve a stream of events from the underlying data store. This stream should take care of pagination and
     * cleanup of any underlying resources (e.g. closing connections if required).
     * @param key The key
     * @param fromSeq The starting sequence to get events from. None to get from the start.
     * @return Stream of events.
     */
    def get(key: K, fromSeq: Option[S] = None): Process[F, Event]

    /**
     * Save the given event.
     *
     * @return Either an Error or the event that was saved. Other non-specific errors should be available
     *         through the container F.
     */
    def put(event: Event): F[Error \/ Event]
  }

  /**
   * Implementations of this interface deal with persisting snapshots so that they don't need to be recomputed every time.
   * Specifically, implementations do NOT deal with generating snapshots, only storing/retrieving any persisted snapshot.
   * @tparam F Container around operations on an underlying data store e.g. Task.
   * @tparam KK The type of the key for snapshots. This does not need to be the same as for the event stream itself.
   * @tparam V The type of the value wrapped by Snapshots that this store persists.
   */
  trait SnapshotStorage[F[_], KK, V] {
    /**
     * Retrieve a snapshot before the given sequence number. We typically specify a sequence number if we want to get
     * some old snapshot i.e. the latest persisted snapshot may have been generated after the point in time that we're
     * interested in.
     *
     * @param key The key
     * @param sequence What sequence we want to get the snapshot for (earliest snapshot, latest, or latest before some sequence)
     * @return The snapshot, a NoSnapshot if there was no snapshot for the given conditions.
     */
    def get(key: KK, sequence: SequenceQuery[S]): F[Snapshot[V]]

    /**
     * Save a given snapshot
     * @param snapshotKey The key
     * @param snapshot The snapshot to save
     * @return Either a Throwable (for error) or the saved snapshot.
     */
    def put(snapshotKey: KK, snapshot: Snapshot[V]): F[Throwable \/ Snapshot[V]]
  }

  /**
   * Dummy implementation that does not store snapshots.
   */
  object SnapshotStorage {
    def none[F[_]: Monad, KK, V]: SnapshotStorage[F, KK, V] =
      new NoSnapshotStorage

    private class NoSnapshotStorage[F[_]: Monad, KK, V] extends SnapshotStorage[F, KK, V] {
      def get(key: KK, sequence: SequenceQuery[S]): F[Snapshot[V]] =
        Snapshot.zero[V].point[F]

      def put(key: KK, snapshot: Snapshot[V]): F[Throwable \/ Snapshot[V]] =
        snapshot.right[Throwable].point[F]
    }
  }

  /**
   * This is the main interface for consumers of the Event source. Implementations of this wrap logic for generating
   * a view of data or aggregate (wrapped in a Snapshot) from an event stream of E events.
   *
   * Upon construction of an API, a suitable Events store, snapshot store, accumulator, and key transform
   * need to be provided.
   *
   * @tparam F Container type for API operations. It needs to be a Monad and a Catchable (e.g. scalaz Task)
   * @tparam KK Key type for the aggregate (may be different from the key for the event stream)
   * @tparam V Aggregate type
   */
  trait API[F[_], KK, V] {
    import scalaz.syntax.monad._

    protected implicit def M: Monad[F]
    protected implicit def C: Catchable[F]

    /**
     * @return Underlying store of events
     */
    def eventStore: EventStorage[F]

    /**
     * @return Underlying store of snapshots
     */
    def snapshotStore: SnapshotStorage[F, KK, V]

    /**
     * Accumulator function for generating a view of the data from a previous view and a new event.
     * @param v The previous view
     * @param e Event to add to previous view
     * @return New view with event added.
     */
    def acc(key: KK)(v: Snapshot[V], e: Event): Snapshot[V]

    /**
     * Transform a given aggregate key to the key for the underlying event stream
     * @param apiKey The aggregate key
     * @return event stream key
     */
    def eventStreamKey(apiKey: KK): K

    /**
     * Return the current view of the data for key 'key'
     */
    final def get(key: KK): F[Option[V]] =
      getSnapshot(key).map { _.value }

    /**
     * @return the current view wrapped in Snashot of the data for key 'key'
     */
    final def getSnapshot(key: KK): F[Snapshot[V]] =
      getSnapshotAt(key, None)

    /**
     * @param key The key of the aggregate to retrieve
     * @param at If none, get me the latest. If some, get me the snapshot at that specific sequence.
     * @return A Snapshot for the aggregate at the given sequence number.
     */
    private def getSnapshotAt(key: KK, at: Option[S]): F[Snapshot[V]] =
      for {
        persistedSnapshot <- snapshotStore.get(key, at.fold(SequenceQuery.latest[S])(SequenceQuery.before))
        fromSeq = persistedSnapshot.id.map { _.sequence }
        pred = at.fold[Event => Boolean] { _ => true } { seq => e => S.order.lessThanOrEqual(e.id.sequence, seq) }
        events = eventStore.get(eventStreamKey(key), fromSeq).takeWhile(pred)
        theSnapshot <- generateSnapshot(persistedSnapshot, events, acc(key))
      } yield theSnapshot

    /**
     * Return the view of the data for the key 'key' at the specified sequence number.
     * @param key the key
     * @param seq the sequence number of the event at which we want the see the view of the data.
     * @return view of the data at event with sequence 'seq'
     */
    final def getAt(key: KK, seq: S): F[Option[V]] =
      getSnapshotAt(key, Some(seq)).map { _.value }

    /**
     * Get a stream of Snapshots starting from sequence number 'from' (if defined).
     * @param key The key
     * @param from Starting sequence number. None to get from the beginning of the stream.
     * @return a stream of Snapshots starting from sequence number 'from' (if defined).
     */
    final def getHistory(key: KK, from: Option[S]): F[Process[F, Snapshot[V]]] =
      for {
        startingSnapshot <- getSnapshotAt(key, from)
      } yield eventStore.get(eventStreamKey(key), startingSnapshot.id.map { _.sequence })
        .scan[Snapshot[V]](startingSnapshot) {
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
    final def getAt(key: KK, time: DateTime): F[Option[V]] = {
      import com.github.nscala_time.time.Implicits._
      // We need to get the earliest snapshot, then the stream of events from that snapshot
      for {
        earliestSnapshot <- snapshotStore.get(key, SequenceQuery.earliest[S])
        value <- generateSnapshot(earliestSnapshot, eventStore.get(eventStreamKey(key), earliestSnapshot.id.map { _.sequence }).takeWhile { _.time <= time }, acc(key)).map { _.value }
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
    final def save(key: KK, operation: Operation[V]): F[SaveResult[V]] =
      for {
        old <- getSnapshot(key)
        op = operation.run(old)
        result <- op.fold(Error.noop.left[Event].point[F], Error.reject(_).left[Event].point[F], v => eventStore.put(Event.next(eventStreamKey(key), old, v)))
        transform <- result match {
          case -\/(Error.DuplicateEvent) =>
            save(key, operation)
          case -\/(Error.Noop) =>
            SaveResult.noop[V]().point[F]
          case -\/(Error.Rejected(r)) =>
            SaveResult.reject[V](r).point[F]
          case \/-(event) =>
            SaveResult.success(acc(key)(old, event)).point[F]
        }
      } yield transform

    /**
     * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
     * @param events The stream of events.
     * @return Container F that when executed provides the snapshot.
     */
    private def generateSnapshot(start: Snapshot[V], events: Process[F, Event], f: (Snapshot[V], Event) => Snapshot[V]): F[Snapshot[V]] =
      events.pipe {
        process1.fold(start)(f)
      }.runLastOr(start)
  }
}
