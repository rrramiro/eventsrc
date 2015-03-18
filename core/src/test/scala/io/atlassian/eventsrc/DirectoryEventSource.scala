package io.atlassian.eventsrc

import org.scalacheck.{Gen, Arbitrary, Prop}
import org.specs2.{ScalaCheck, SpecificationWithJUnit}

import scalaz.{Catchable, Monad, \/}
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.nel._
import Arbitrary.arbitrary

/**
 * TODO: Document this file here
 */
object DirectoryEventSource {
  type DirectoryId = String
  type ZoneId = Long
}

sealed trait DirectoryEvent
case class AddUser(user: User) extends DirectoryEvent

case class User(id: String, username: String)

class DirectoryEventSource(zone: DirectoryEventSource.ZoneId) extends EventStream[DirectoryEventSource.DirectoryId, TwoPartSequence, DirectoryEvent] {
  import DirectoryEventSource._
  import EventStream.Error

  override lazy val S = TwoPartSequence.twoPartSequence(zone)

  class MemoryEventStorage extends EventStorage[Task] {
    val map = collection.concurrent.TrieMap[DirectoryId, List[Event]]()

    override def get(key: DirectoryId, fromOption: Option[TwoPartSequence]): Process[Task, Event] =
      map.get(key) match {
        case None     => Process.halt
        case Some(cs) => Process.emitAll(cs.reverse.takeWhile { ev => fromOption.fold(true) { from => S.order.greaterThanOrEqual(from, ev.id.sequence) } })
      }

    override def put(ev: Event): Task[Error \/ Event] =
      Task {
        // Just assume there are no duplicates for this test and that everything is ordered when I get it
        map += (ev.id.key -> (ev :: map.getOrElse(ev.id.key, Nil)))
        ev.right
      }
  }

  class AllUsersSnapshotStorage extends SnapshotStorage[Task, List[User]] {
    val map = collection.concurrent.TrieMap[DirectoryId, Snapshot[List[User]]]()

    def get(key: DirectoryId, sequence: SequenceQuery[TwoPartSequence]): Task[Snapshot[List[User]]] =
      Task {
        map.getOrElse(key, Snapshot.zero)
      }

    def put(key: DirectoryId, view: Snapshot[List[User]]): Task[Throwable \/ Snapshot[List[User]]] =
      Task {
        map += (key -> view)
        view.right
      }
  }

  class AllUsersAPI(val eventStore: EventStorage[Task], val snapshotStore: SnapshotStorage[Task, List[User]])(implicit val M: Monad[Task], val C: Catchable[Task]) extends API[Task, List[User]] {
    override def acc(v: Snapshot[List[User]], e: Event): Snapshot[List[User]] =
      e.operation match {
        case AddUser(user) =>
          val userList: List[User] =
            v.value.fold(List(user)) { l => user :: l }

          Snapshot.Value(userList, e.id, e.time)
      }
  }
}

class DirectoryEventSourceSpec extends SpecificationWithJUnit with ScalaCheck {
  import DirectoryEventSource._

  def is =
    s2"""
        DirectoryEventSource supports
          Adding multiple users           $addMultipleUsers
      """

  lazy val eventSource = new DirectoryEventSource(1)
  lazy val api = new eventSource.AllUsersAPI(new eventSource.MemoryEventStorage, new eventSource.AllUsersSnapshotStorage)

  implicit val ArbitraryUser: Arbitrary[User] =
    Arbitrary(
      for {
        uid <- Gen.uuid
        name <- arbitrary[String]
      } yield User(uid.toString, name)
    )

  def addMultipleUsers = Prop.forAll { (k: DirectoryId, u1: User, u2: User) => u1.username != u2.username ==> {
    import eventSource._
    import eventSource.Operation._

    def addUniqueUser(u: User): eventSource.Operation[List[User]] =
      Operation {
        _.fold(Result.Success(AddUser(u))
          , (l, _, _) =>
            if (l.exists {
              _.username == u.username
            })
              Result.Reject(Reason("User with same username already exists").wrapNel)
            else
              Result.Success(AddUser(u))
          , (_, _) =>
            Result.Success(AddUser(u))
        )
      }

    (for {
      x <- api.save(k, addUniqueUser(u1))
      x <- api.save(k, addUniqueUser(u2))
      allUsers <- api.get(k)
    } yield allUsers).run === Some(List(u2, u1))
  }
  }
}

/**
 * Idea for sharding:
 *
 * We have an API[Task, Prefix => Map[Username, UserId] ]
 * i.e. V is a function
 *
 * Q) Do we want to have a different key for snapshot storage? Or provide other parameters?
 * SnapshotStorage -
 *  - put - How can we store a snapshot when it is a function?
 *
 * We actually want multiple APIs/SnapshotStorages - one for each prefix. We can create a new API for a request with a prefix specified.
 *
 *
 * Q) Do we want to hash prefix? This will provide better balancing
 *
 * Q) Do we want to have snapshot keyed by something else? Is my API actually using the same Key?
 *
 * EventStream[K, S, E] - K helps identify the stream
 * EventStorage - This needs to have the same K
 *
 * Snapshot and API - this is an independent key. How does this work?
 *  - These need to be tied to an EventStream. We need to know how to get a key from whatever thing we get as input
 *
 * e.g. to add a user
 *  - check for username uniqueness
 *  - check for user email uniqueness. I want a map of all user emails. Specifically I want the user Id for a given email
 *    - My API is actually get(email: Email): Option[UserId].
 *      - This would get a Snapshot for email.
 *        - Snapshot would be Snapshot[UserId]. The Snapshot storage implementation would get the prefix of the email, lookup the appropriate record and return.
 *      - In this case it would get an event stream for all users (K = 'all').
 *  -
 *
 * Q) How to update snapshots then? An event stream would need to know about all the snapshot storages.
 */