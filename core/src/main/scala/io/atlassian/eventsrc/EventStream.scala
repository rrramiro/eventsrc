package io.atlassian.eventsrc

import org.joda.time.DateTime

import scalaz._
import scalaz.stream.{process1, Process}
import scalaz.syntax.either._

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
trait EventStream[K, S, E] {
  import EventStream._
  
  def S: Sequence[S]

  /**
   * A event is identified by the key and an incrementing sequence number
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

  case class Event(id: EventId, time: DateTime, operation: E)

  object Event {
    def next[A](key: K, snapshot: Snapshot[A], op: E): Event =
      Event(snapshot.id.map { EventId.next }.getOrElse { EventId.first(key) }, DateTime.now, op)
  }

  /**
   * A Snapshot wraps an optional value, and tags it with an event Id. We can say a 'snapshot' S of key K at event
   * S.at is value S.value
   *
   * The event Id is quite a useful thing in addition to the value of the snapshot.
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
     * @param at
     */
    case class Value[V](view: V, at: EventId, time: DateTime) extends Snapshot[V] {
      val value = Some(view)
    }

    /**
     * Events have been saved and there is no value (i.e. the value has been deleted).
     * @param at
     */
    case class Deleted[V](at: EventId, time: DateTime) extends Snapshot[V] {
      val value = None
    }

    def zero[V]: Snapshot[V] = NoSnapshot[V]()
  }

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


  case class Operation[V](run: Snapshot[V] => Operation.Result[V])

  object Operation {
    sealed trait Result[A] {
      import Result._

      // TODO - Do we need this?
//      def map[B](f: A => B): Result[B] =
//        this match {
//          case Success(t) => Success(t map f)
//          case Reject(r)  => Reject(r)
//          case Noop()     => Noop()
//        }

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

//      implicit object ResultFunctor extends Functor[Result] {
//        def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa map f
//      }
    }
  }

  /**
   * A source of events. Implementations wrap around an underlying data store (e.g. in-memory map or DynamoDB).
   *
   * @tparam F Container around operations on an underlying data store. F must be a Monad and a Catchable (e.g. Task).
   */
  trait EventStorage[F[_]] {
    /**
     * Retrieve a stream of events from the underlying data store. This stream should take care of pagination and
     * cleanup of any underlying resources (e.g. closing connections if required).
     * @param key The key
     * @param fromSeq The starting sequence to get events from
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
   * @tparam F
   * @tparam V
   */
  trait SnapshotStorage[F[_], V] {
    def get(key: K, beforeSeq: Option[S] = None): F[Snapshot[V]]

    def put(key: K, view: Snapshot[V]): F[Throwable \/ Snapshot[V]]
  }

  /**
   * This is the main interface for consumers of the Event source.
   *
   * Implementations contain logic to create a transform given a value to save.
   *
   * Upon construction of an API, a suitable Events store needs to be provided.
   *
   * @tparam F Container type for API operations. It needs to be a Monad and a Catchable (e.g. scalaz Task)
   */
  trait API[F[_], V] {
    import scalaz.syntax.monad._

    protected implicit def M: Monad[F]
    protected implicit def C: Catchable[F]

    /**
     * @return Underlying store of events
     */
    def eventStore: EventStorage[F]
    def snapshotStore: SnapshotStorage[F, V]

    def acc(v: Snapshot[V], e: Event): Snapshot[V]

    /**
     * Return the current view of the data for key 'key'
     */
    final def get(key: K): F[Option[V]] =
      getSnapshot(key).map { _.value }

    final def getSnapshot(key: K): F[Snapshot[V]] =
      getSnapshotAt(key, None)

    /**
     *
     * @param key
     * @param at If none, get me the latest. If some, get me the snapshot at that specific sequence.
     * @return
     */
    private def getSnapshotAt(key: K, at: Option[S]): F[Snapshot[V]] =
      for {
        persistedSnapshot <- snapshotStore.get(key, at)
        fromSeq = persistedSnapshot.id.map { _.sequence }
        pred = at.fold[Event => Boolean] { _ => true } { seq => e => S.order.lessThanOrEqual(e.id.sequence, seq) }
        events = eventStore.get(key, fromSeq).takeWhile(pred)
        theSnapshot <- generateSnapshot(persistedSnapshot, events)
      } yield theSnapshot

    /**
     * Return the view of the data for the key 'key' at the specified sequence number.
     * @param key the key
     * @param seq the sequence number of the event at which we want the see the view of the data.
     * @return view of the data at event with sequence 'seq'
     */
    final def getAt(key: K, seq: S): F[Option[V]] =
      getSnapshotAt(key, Some(seq)).map { _.value }
//
//    final def getHistory(key: K): F[Process[F, Snapshot]] =
//      M.point(eventStore.get(key).scan[Snapshot](Snapshot.zero) {
//        case (snapshot, event) => Snapshot.update(snapshot, event)
//      }.drop(1)) // skip the initial value of Snapshot.zero

    /**
     * Return the view of the data for the key 'key' at the specified timestamp.
     * @param key The key
     * @param time The timestamp at which we want to see the view of the data
     * @return view of the data with events up to the given time stamp.
     */
//    final def getAt(key: K, time: DateTime): F[Option[V]] = {
//      import com.github.nscala_time.time.Implicits._
//      getWhile(key) { _.time <= time }
//    }

    /**
     * Save a value for the given key.
     *
     * Pass in the function that creates a possible transform,
     * given the current value if there is one.
     */
    /*
     * To save a new value, we need to get the latest snapshot in order to get the existing view of data and the
     * latest event Id. Then we create a suitable transform and event and try to save it. Upon duplicate event,
     * try the operation again (highly unlikely that this situation would occur).
     */
    final def save(key: K, operation: Operation[V]): F[SaveResult[V]] =
      for {
        old <- getSnapshot(key)
        op = operation.run(old)
        result <- op.fold(Error.noop.left[Event].point[F], Error.reject(_).left[Event].point[F], v => eventStore.put(Event.next(key, old, v)))
        transform <- result match {
          case -\/(Error.DuplicateEvent) =>
            save(key, operation)
          case -\/(Error.Noop) =>
            SaveResult.noop[V]().point[F]
          case -\/(Error.Rejected(r)) =>
            SaveResult.reject[V](r).point[F]
          case \/-(event) =>
            SaveResult.success(acc(old, event)).point[F]
        }
      } yield transform

    /**
     * All a 'get' is doing is taking events up to a condition (e.g. sequence number or a date) and then applying
     * them in order. This is quite trivial using something like Scalaz Stream.
     * @param key The key for which to retrieve events.
     * @param pred Predicate for filtering events.
     * @return View of the data obtained from applying all events in the stream up until the given condition is not met.
     */
//    private[eventsrc] def getWhile(key: K)(pred: Event => Boolean): F[Option[V]] =
//      M(generateSnapshot(eventStore.get(key).takeWhile(pred))) { _.value }

    /**
     * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
     * @param events The stream of events.
     * @return Container F that when executed provides the snapshot.
     */
    private def generateSnapshot(start: Snapshot[V], events: Process[F, Event]): F[Snapshot[V]] =
      events.pipe {
        process1.fold(start)(acc)
      }.runLastOr(start)
  }  
}
