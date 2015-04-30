package io.atlassian.event
package source

import org.joda.time.DateTime

import scalaz.{ -\/, Catchable, Monad, NonEmptyList, \/, \/- }
import scalaz.stream.{ Process, process1 }
import scalaz.syntax.either._
import scalaz.syntax.monad._

/**
 * An event source is an append-only store of data. Data is represented as a series of events that when replayed in
 * order provides a view of the data at that point in time.
 *
 * In this implementation, an event is represented by a Transform that is contained within an event (strictly speaking
 * a Event could contain a series of Transforms, but we're not doing that here to keep things simple).
 *
 * To implement an event source, one needs to:
 *   - Extend the EventSource trait. An event source provides data of type V for a given key of type K
 *   - Provide an API implementation that creates a suitable Transform for a new value to be saved when given an existing
 *     view of the data.
 *   - Provide Events implementation that wraps persistence of events (e.g. DynamoDB, in-memory map, Cassandra).
 *     Persistence stores only need to support the following key-value operations:
 *       - Records are keyed against a hash key (typically a unique identifier of business value) and a numeric range key
 *       - querying for a given hash key
 *       - conditional saves against a given hash and range key to prevent overwriting of a given record.
 *
 *  TODO - make EventSource sit on top of EventStream
 */
object EventSource {
  sealed trait Result[A]
  object Result {
    case class Insert[A](n: A) extends Result[A]
    case class Update[A](o: A, n: A) extends Result[A]
    case class Delete[A](o: A) extends Result[A]
    case class Reject[A](reasons: NonEmptyList[Reason]) extends Result[A]
    case class Noop[A]() extends Result[A]

    def insert[A](n: A): Result[A] = Insert(n)
    def update[A](o: A, n: A): Result[A] = Update(o, n)
    def delete[A](o: A): Result[A] = Delete(o)
    def reject[A](reasons: NonEmptyList[Reason]): Result[A] = Reject(reasons)
    def noop[A](): Result[A] = Noop()
  }

  /**
   * EventSource.Error represents any error conditions that are useful to represent for event sources. In particular,
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
 * The main trait that implementations of an event source need to extend.
 * @tparam K The key against which values are stored.
 * @tparam V Values to be store
 * @tparam S Type of the sequence. Needs to have a Sequence type class implementation.
 */
trait EventSource[K, V, S] {
  import EventSource._

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

  case class Event(id: EventId, time: DateTime, operation: Transform[V])

  object Event {
    def next(key: K, snapshot: Snapshot, op: Transform[V]): Event =
      Event(snapshot.id.map { EventId.next }.getOrElse { EventId.first(key) }, DateTime.now, op)
  }

  /**
   * A Snapshot wraps an optional value, and tags it with an event Id. We can say a 'snapshot' S of key K at event
   * S.at is value S.value
   *
   * The event Id is quite a useful thing in addition to the value of the snapshot.
   *
   * This is only used internally within an event source.
   */
  sealed trait Snapshot {
    import Snapshot._
    def value: Option[V]
    def id: Option[EventId] =
      this.fold(None, { case (_, at, _) => Some(at) }, { case (at, _) => Some(at) })

    def fold[X](none: => X, value: (V, EventId, DateTime) => X, deleted: (EventId, DateTime) => X): X =
      this match {
        case NoSnapshot() => none
        case Value(v, at, time) => value(v, at, time)
        case Deleted(at, time) => deleted(at, time)
      }
  }

  object Snapshot {
    /**
     * There is no snapshot... i.e. no events have been saved.
     */
    case class NoSnapshot() extends Snapshot {
      val value = None
    }

    /**
     * Events have been saved and there is a value stored.
     * @param view The value
     * @param at identifier
     * @param time timestamp of the last event.
     */
    case class Value(view: V, at: EventId, time: DateTime) extends Snapshot {
      val value = Some(view)
    }

    /**
     * Events have been saved and there is no value (i.e. the value has been deleted).
     * @param at identifier
     * @param time timestamp of the last event.
     */
    case class Deleted(at: EventId, time: DateTime) extends Snapshot {
      val value = None
    }

    val zero: Snapshot = NoSnapshot()

    def update(s: Snapshot, ev: Event): Snapshot =
      ev.operation.value match {
        case None => Deleted(ev.id, ev.time)
        case Some(v) => Value(v, ev.id, ev.time)
      }
  }

  /**
   * A source of events. Implementations wrap around an underlying data store (e.g. in-memory map or DynamoDB).
   *
   * @tparam F Container around operations on an underlying data store. F must be a Monad and a Catchable (e.g. Task).
   */
  protected[EventSource] trait Storage[F[_]] {
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
   * This is the main interface for consumers of the Event source.
   *
   * Implementations contain logic to create a transform given a value to save.
   *
   * Upon construction of an API, a suitable Events store needs to be provided.
   *
   * @tparam F Container type for API operations. It needs to be a Monad and a Catchable (e.g. scalaz Task)
   */
  trait API[F[_]] {
    protected implicit def M: Monad[F]
    protected implicit def C: Catchable[F]

    /**
     * @return Underlying store of events
     */
    def store: Storage[F]

    /**
     * Return the current view of the data for key 'key'
     */
    final def get(key: K): F[Option[V]] =
      getWhile(key) { _ => true }

    /**
     * Return the view of the data for the key 'key' at the specified sequence number.
     * @param key the key
     * @param seq the sequence number of the event at which we want the see the view of the data.
     * @return view of the data at event with sequence 'seq'
     */
    final def getAt(key: K, seq: S): F[Option[V]] =
      getWhile(key) { e => S.order.lessThanOrEqual(e.id.sequence, seq) }

    final def getHistory(key: K): F[Process[F, Snapshot]] =
      M.point(store.get(key).scan[Snapshot](Snapshot.zero) {
        case (snapshot, event) => Snapshot.update(snapshot, event)
      }.drop(1)) // skip the initial value of Snapshot.zero

    /**
     * Return the view of the data for the key 'key' at the specified timestamp.
     * @param key The key
     * @param time The timestamp at which we want to see the view of the data
     * @return view of the data with events up to the given time stamp.
     */
    final def getAt(key: K, time: DateTime): F[Option[V]] = {
      import com.github.nscala_time.time.Implicits._
      getWhile(key) { _.time <= time }
    }

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
    final def save(key: K, operation: Operation[V]): F[EventSource.Result[V]] =
      for {
        old <- extractSnapshot(store.get(key))
        op = operation.run(old.value)
        result <- op.fold(Error.noop.left[Event].point[F], Error.reject(_).left[Event].point[F], v => store.put(Event.next(key, old, v)))
        transform <- result match {
          case -\/(Error.DuplicateEvent) =>
            save(key, operation)
          case -\/(Error.Noop) =>
            EventSource.Result.noop[V]().point[F]
          case -\/(Error.Rejected(r)) =>
            EventSource.Result.reject[V](r).point[F]
          case \/-(event) =>
            ((old.value, event.operation) match {
              case (None, Transform.Insert(e)) => Result.insert(e)
              case (Some(o), Transform.Insert(n)) => Result.update(o, n)
              case (Some(o), Transform.Delete) => Result.delete(o)
              case (None, Transform.Delete) => Result.noop[V]() // shouldn't happen, only here for exhaustiveness
            }).point[F]
        }
      } yield transform

    /**
     * All a 'get' is doing is taking events up to a condition (e.g. sequence number or a date) and then applying
     * them in order. This is quite trivial using something like Scalaz Stream.
     * @param key The key for which to retrieve events.
     * @param pred Predicate for filtering events.
     * @return View of the data obtained from applying all events in the stream up until the given condition is not met.
     */
    private[source] def getWhile(key: K)(pred: Event => Boolean): F[Option[V]] =
      M(extractSnapshot(store.get(key).takeWhile(pred))) { _.value }

    /**
     * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
     * @param events The stream of events.
     * @return Container F that when executed provides the snapshot.
     */
    private def extractSnapshot(events: Process[F, Event]): F[Snapshot] =
      events.pipe {
        process1.fold(Snapshot.zero)(Snapshot.update)
      }.runLastOr(Snapshot.zero)
  }
}
