package io.atlassian.throng

import io.atlassian.eventsrc.Sequence
import org.joda.time.DateTime

import scalaz.concurrent.Task
import scalaz.stream.{Process, process1}
import scalaz.{Catchable, Monad, \/}

/**
 * 1. Support add/remove/update organisation
 * - Need to be able to add new organisations with no clashing organisation name
 * - Organisation has a set of owners and admins (user Ids). These need to change over time
 * - We need to support merging of organisations
 *
 *
 * Q) Do we store owners/admins as part of Organisations, Organisation or Directory streams?
 * - We do want atomicity in the creation of an Organisation. Once a record is created in Organisations, that is
 *
 * Q) Is the Organisation stream the master for an Organisation? What is the purpose of the Organisations stream?
 * - To provide a sync point
 *
 * We want an Event store, and several views - views have a snapshot store, link to event store. View is basically what API provides
 */
object OrgTest {

  sealed trait OrganisationsEvent
  case class AddOrganisation(id: String, name: String) extends OrganisationsEvent

  case class Organisation(id: String, name: String)

  class OrganisationsEventStream extends EventStream[String, Long, OrganisationsEvent] {
    override lazy val S: Sequence[Long] = implicitly[Sequence[Long]]
  }

  sealed trait DirectoryEvent
  case class AddUser(id: String, name: String) extends DirectoryEvent
  case class AddGroup(id: String, name: String) extends DirectoryEvent

  class DirectoryEventStream extends EventStream[String, Long, DirectoryEvent] {
    override lazy val S: Sequence[Long] = implicitly[Sequence[Long]]
  }

  case class Group(id: String)
  val bar = new DirectoryEventStream
  trait GroupDao extends bar.DAO[Task, Group] {

  }

  val foo = new OrganisationsEventStream

  class MemoryEventStorage extends foo.EventStorage[Task] {
    override def get(key: String, fromSeq: Option[Long] = None): Process[Task, foo.Event] = ???
    override def put(event: foo.Event): Task[EventStream.Error \/ foo.Event] = ???
  }

  trait OrganisationListDao extends foo.DAO[Task, List[Organisation]] {
    def acc(v: foo.View[List[Organisation]], e: foo.Event): foo.View[List[Organisation]] = ???
  }

  object EventStream {
    sealed trait Error
    sealed trait Result[A]
  }

  // Create an event source (definition? event stream?) for a key, sequence and event type
  // An event stream defines the types and interfaces.
  // We would need to provide implementations for Event Storage, View Cache, and DAO (which joins them together)
  //
  trait EventStream[K, S, E] {
    def S: Sequence[S]

    case class EventId(key: K, sequence: S)

    case class Event(id: EventId, time: DateTime, operation: E)

    // View needs to be defined here because it relates to an EventId
    sealed trait View[V] {

      import View._

      def value: Option[V]

      def id: Option[EventId] =
        this.fold(None, { case (_, at, _) => Some(at)}, { case (at, _) => Some(at)})

      def fold[X](none: => X, value: (V, EventId, DateTime) => X, deleted: (EventId, DateTime) => X): X =
        this match {
          case NoView() => none
          case Value(v, at, time) => value(v, at, time)
          case Deleted(at, time) => deleted(at, time)
        }
    }

    object View {

      case class NoView[V]() extends View[V] {
        val value = None
      }

      case class Value[V](view: V, at: EventId, time: DateTime) extends View[V] {
        val value = Some(view)
      }

      case class Deleted[V](at: EventId, time: DateTime) extends View[V] {
        val value = None
      }

      def zero[V]: View[V] = NoView[V]()
    }

    trait EventStorage[F[_]] {
      def get(key: K, fromSeq: Option[S] = None): Process[F, Event]

      def put(event: Event): F[EventStream.Error \/ Event]
    }

    trait ViewCache[F[_], V] {
      def get(key: K, beforeSeq: Option[S] = None): F[View[V]]

      def put(key: K, view: View[V]): F[Throwable \/ View[V]]
    }

    case class Operation[A](run: View[A] => Operation.Result[A])

    object Operation {

      sealed trait Result[A]

    }

    trait DAO[F[_], V] {

      import scalaz.syntax.monad._

      protected implicit def M: Monad[F]

      protected implicit def C: Catchable[F]

      def eventStore: EventStorage[F]

      def viewStore: ViewCache[F, V]

      // TODO - this needs to be in View I think because it is tied to an Event Stream and a specific view
      def acc(v: View[V], e: Event): View[V]

      final def get(key: K): F[Option[V]] =
        getView(key).map {
          _.value
        }

      final def getView(key: K): F[View[V]] =
        for {
          lastView <- viewStore.get(key, None)
          fromSeq = lastView.id.map {
            _.sequence
          }
          events = eventStore.get(key, fromSeq)
          view <- extractView(lastView, events)
        } yield view

      final def getViewAt(key: K, seq: S): F[View[V]] =
        for {
          lastView <- viewStore.get(key, Some(seq))
          fromSeq = lastView.id.map {
            _.sequence
          }
          events = eventStore.get(key, fromSeq)
          view <- extractView(lastView, events)
        } yield view

      final def getHistory(key: K, from: S): F[Process[F, View[V]]] =
        for {
          lastView <- getViewAt(key, from)
        } yield eventStore.get(key, lastView.id.map {
          _.sequence
        }).scan[View[V]](lastView) {
          case (view, event) => acc(view, event)
        }.drop(1)

      def save(key: K, operation: Operation[V]): F[EventStream.Result[V]]

      private def extractView(start: View[V], events: Process[F, Event]): F[View[V]] =
        events.pipe {
          process1.fold(start)(acc)
        }.runLastOr(start)

    }

  }

}
