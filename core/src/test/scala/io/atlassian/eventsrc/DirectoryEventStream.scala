package io.atlassian.eventsrc

import org.joda.time.DateTime
import org.scalacheck.{ Gen, Arbitrary, Prop }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.{ Catchable, Monad, \/ }
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.nel._
import Arbitrary.arbitrary

/**
 * TODO: Document this file here
 */
object DirectoryEventStream {
  type DirectoryId = String
  type ZoneId = Long
  type Username = String
  type UserId = String
  type DirectoryUsername = (DirectoryId, Username)
  type DirectoryUsernamePrefix = (DirectoryId, String)
}

sealed trait DirectoryEvent
case class AddUser(user: User) extends DirectoryEvent

import DirectoryEventStream._

case class User(id: UserId, username: Username)

class DirectoryEventStream(zone: ZoneId) extends EventStream {
  import EventStream.Error

  type K = DirectoryId
  type S = TwoPartSequence
  type E = DirectoryEvent

  override lazy val S = TwoPartSequence.twoPartSequence(zone)

  object MemoryEventStorage extends EventStorage[Task] {
    val map = collection.concurrent.TrieMap[DirectoryId, List[Event]]()

    override def get(key: DirectoryId, fromOption: Option[TwoPartSequence]): Process[Task, Event] =
      map.get(key) match {
        case None => Process.halt
        case Some(cs) =>
          Process.emitAll(cs.reverse.dropWhile { ev => fromOption.fold(false) { from => S.order.lessThanOrEqual(ev.id.sequence, from) } })
      }

    override def put(ev: Event): Task[Error \/ Event] =
      Task {
        // Just assume there are no duplicates for this test and that everything is ordered when I get it
        map += (ev.id.key -> (ev :: map.getOrElse(ev.id.key, Nil)))
        ev.right
      }
  }

  class ShardedUsernameToIdMappingSnapshotStorage extends SnapshotStorage[Task, DirectoryUsername, UserId] {
    val map = collection.concurrent.TrieMap[DirectoryUsernamePrefix, Snapshot[Map[Username, UserId]]]()
    def get(key: DirectoryUsername, seq: SequenceQuery[TwoPartSequence]): Task[Snapshot[UserId]] =
      Task {
        map.getOrElse(prefix(key), Snapshot.zero).fold(Snapshot.zero[UserId],
          (m, id, t) =>
            m.get(key._2).fold(Snapshot.deleted[UserId](id, t)) { uid => Snapshot.value(uid, id, t) },
          (id, t) => Snapshot.deleted(id, t)) // This should not happen
      }

    def put(key: DirectoryUsername, view: Snapshot[UserId]): Task[Throwable \/ Snapshot[UserId]] =
      Task {
        map.get(prefix(key)) match {
          case None =>
            val newSnapshot: Snapshot[Map[Username, UserId]] =
              view.fold(Snapshot.zero[Map[Username, UserId]], (uid, id, t) => Snapshot.value(Map(key._2 -> uid), id, t), (id, t) => Snapshot.value(Map(), id, t))
            map += (prefix(key) -> newSnapshot)
          case Some(s) =>
            (s, view) match {
              case (Snapshot.Value(m, id1, t1), Snapshot.Value(uid, id2, t2)) =>
                map += (prefix(key) -> Snapshot.Value(m + (key._2 -> uid), id2, t2))
              case (Snapshot.Value(m, id1, t1), Snapshot.Deleted(id2, t2)) =>
                map += (prefix(key) -> Snapshot.Value(m - key._2, id2, t2))
              case _ =>
                ()
            }
        }
        view.right
      }

    private def prefix(key: DirectoryUsername): (DirectoryId, String) =
      (key._1, key._2.substring(0, Math.min(3, key._2.length)))
  }

  class ShardedUsernameAPI(val snapshotStore: SnapshotStorage[Task, DirectoryUsername, UserId])(implicit val M: Monad[Task], val C: Catchable[Task]) extends API[Task, DirectoryUsername, UserId] {
    override val eventStore: EventStorage[Task] = MemoryEventStorage
    override def eventStreamKey(k: DirectoryUsername): DirectoryId =
      k._1

    override def acc(key: DirectoryUsername)(v: Snapshot[UserId], e: Event): Snapshot[UserId] =
      e.operation match {
        case AddUser(user) =>
          if (key._2 == user.username)
            Snapshot.value(user.id, e.id, e.time)
          else
            v
      }
  }
  class AllUsersSnapshotStorage extends SnapshotStorage[Task, DirectoryId, List[User]] {
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

  class AllUsersAPI(val snapshotStore: SnapshotStorage[Task, DirectoryId, List[User]])(implicit val M: Monad[Task], val C: Catchable[Task]) extends API[Task, DirectoryId, List[User]] {
    override val eventStore: EventStorage[Task] = MemoryEventStorage
    override def eventStreamKey(k: DirectoryId): DirectoryId =
      k

    override def acc(key: DirectoryId)(v: Snapshot[List[User]], e: Event): Snapshot[List[User]] =
      e.operation match {
        case AddUser(user) =>
          val userList: List[User] =
            v.value.fold(List(user)) { l => user :: l }

          Snapshot.Value(userList, e.id, e.time)
      }
  }
}

// Test - Add multiple users, Add duplicate users, use sharded storage, snapshot storage
class DirectoryEventStreamSpec extends SpecificationWithJUnit with ScalaCheck {
  import DirectoryEventStream._

  def is =
    s2"""
        DirectoryEventStream supports
          Adding multiple users (store list of users) $addMultipleUsers
          Checking for duplicate usernames (store list of users) $duplicateUsername
          Adding multiple users (store list of users and snapshot) $addMultipleUsersWithSnapshot
          Checking for duplicate usernames (store list of users and snapshot) $duplicateUsernameWithSnapshot

          Checking for duplicate usernames with sharded store $duplicateUsernameSharded
          Checking for duplicate usernames with sharded store and snapshot $duplicateUsernameShardedWithSnapshot

      """

  implicit val ArbitraryUser: Arbitrary[User] =
    Arbitrary(
      for {
        uid <- Gen.uuid
        name <- arbitrary[String]
      } yield User(uid.toString, name)
    )

  def addMultipleUsers = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val eventStream = new DirectoryEventStream(1)
      val api = new eventStream.AllUsersAPI(new eventStream.AllUsersSnapshotStorage)

      import eventStream._
      import eventStream.Operation._

      def addUniqueUser(u: User): eventStream.Operation[List[User]] =
        Operation {
          _.value.fold(Result.success[List[User]](AddUser(u))) { l =>
            if (l.exists { _.username == u.username })
              Result.reject(Reason("User with same username already exists").wrapNel)
            else
              Result.success(AddUser(u))
          }
        }

      (for {
        _ <- api.save(k, addUniqueUser(u1))
        _ <- api.save(k, addUniqueUser(u2))
        allUsers <- api.get(k)
      } yield allUsers).run.get must containTheSameElementsAs(List(u2, u1))
    }
  }

  def duplicateUsername = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = new DirectoryEventStream(1)
    val api = new eventStream.AllUsersAPI(new eventStream.AllUsersSnapshotStorage)

    import eventStream._
    import eventStream.Operation._

    def addUniqueUser(u: User): eventStream.Operation[List[User]] =
      Operation {
        _.value.fold(Result.success[List[User]](AddUser(u))) { l =>
          if (l.exists { _.username == u.username })
            Result.reject(Reason("User with same username already exists").wrapNel)
          else
            Result.success(AddUser(u))
        }
      }

    val user2ToSave = u2.copy(username = u1.username)
    (for {
      _ <- api.save(k, addUniqueUser(u1))
      _ <- api.save(k, addUniqueUser(user2ToSave))
      allUsers <- api.get(k)
    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
  }

  def addMultipleUsersWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val eventStream = new DirectoryEventStream(1)
      val snapshotStorage = new eventStream.AllUsersSnapshotStorage
      val api = new eventStream.AllUsersAPI(snapshotStorage)

      import eventStream._
      import eventStream.Operation._

      def addUniqueUser(u: User): eventStream.Operation[List[User]] =
        Operation {
          _.value.fold(Result.success[List[User]](AddUser(u))) { l =>
            if (l.exists { _.username == u.username })
              Result.reject(Reason("User with same username already exists").wrapNel)
            else
              Result.success(AddUser(u))
          }
        }

      (for {
        _ <- api.save(k, addUniqueUser(u1))
        _ <- snapshotStorage.put(k, Snapshot.value(List(u1), EventId(k, eventStream.S.first), DateTime.now))
        _ <- api.save(k, addUniqueUser(u2))
        allUsers <- api.get(k)
      } yield allUsers).run.get must containTheSameElementsAs(List(u2, u1))
    }
  }

  def duplicateUsernameWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = new DirectoryEventStream(1)
    val snapshotStorage = new eventStream.AllUsersSnapshotStorage
    val api = new eventStream.AllUsersAPI(snapshotStorage)

    import eventStream._
    import eventStream.Operation._

    def addUniqueUser(u: User): eventStream.Operation[List[User]] =
      Operation {
        _.value.fold(Result.success[List[User]](AddUser(u))) { l =>
          if (l.exists { _.username == u.username })
            Result.reject(Reason("User with same username already exists").wrapNel)
          else
            Result.success(AddUser(u))
        }
      }

    val user2ToSave = u2.copy(username = u1.username)
    (for {
      _ <- api.save(k, addUniqueUser(u1))
      _ <- snapshotStorage.put(k, Snapshot.value(List(u1), EventId(k, eventStream.S.first), DateTime.now))
      _ <- api.save(k, addUniqueUser(user2ToSave))
      allUsers <- api.get(k)
    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
  }

  def duplicateUsernameSharded = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = new DirectoryEventStream(1)
    val snapshotStorage = new eventStream.ShardedUsernameToIdMappingSnapshotStorage
    val api1 = new eventStream.AllUsersAPI(new eventStream.AllUsersSnapshotStorage)
    val api2 = new eventStream.ShardedUsernameAPI(snapshotStorage)

    import eventStream._
    import eventStream.Operation._

    def addUniqueUser(u: User): eventStream.Operation[List[User]] =
      Operation {
        _.value.fold(Result.success[List[User]](AddUser(u))) { l =>
          if (l.exists { _.username == u.username })
            Result.reject(Reason("User with same username already exists").wrapNel)
          else
            Result.success(AddUser(u))
        }
      }

    val user2ToSave = u2.copy(username = u1.username)

    // Do the saving operation. This is kind of ugly but supports multiple conditions
    def saveUser(u: User) =
      for {
        s <- api2.getSnapshot((k, u.username))
        op <- s.fold(
          Task.now(Operation[UserId] { _ => Result.success[UserId](AddUser(u)) }),
          (_, _, _) => Task.fail(new Exception("Duplicate username")),
          (id, _) => Task.now(Operation[UserId] { s2 => if (s.id == s2.id) Result.success[UserId](AddUser(u)) else Result.reject(Reason("Locking exception").wrapNel) })
        )
        _ <- api2.save((k, u.username), op)
      } yield ()

    saveUser(u1).attemptRun
    saveUser(user2ToSave).run must throwA[Exception] and
      (api1.get(k).run.get must containTheSameElementsAs(List(u1)))
  }

  def duplicateUsernameShardedWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = new DirectoryEventStream(1)
    val snapshotStorage = new eventStream.ShardedUsernameToIdMappingSnapshotStorage
    val api1 = new eventStream.AllUsersAPI(new eventStream.AllUsersSnapshotStorage)
    val api2 = new eventStream.ShardedUsernameAPI(snapshotStorage)

    import eventStream._
    import eventStream.Operation._

    def addUniqueUser(u: User): eventStream.Operation[List[User]] =
      Operation {
        _.value.fold(Result.success[List[User]](AddUser(u))) { l =>
          if (l.exists { _.username == u.username })
            Result.reject(Reason("User with same username already exists").wrapNel)
          else
            Result.success(AddUser(u))
        }
      }

    val user2ToSave = u2.copy(username = u1.username)

    // Do the saving operation. This is kind of ugly but supports multiple conditions
    def saveUser(u: User) =
      for {
        s <- api2.getSnapshot((k, u.username))
        op <- s.fold(
          Task.now(Operation[UserId] { _ => Result.success[UserId](AddUser(u)) }),
          (_, _, _) => Task.fail(new Exception("Duplicate username")),
          (id, _) => Task.now(Operation[UserId] { s2 => if (s.id == s2.id) Result.success[UserId](AddUser(u)) else Result.reject(Reason("Locking exception").wrapNel) })
        )
        _ <- api2.save((k, u.username), op)
      } yield ()

    saveUser(u1).attemptRun
    // Manually save a snapshot
    snapshotStorage.put((k, u1.username), Snapshot.value(u1.id, EventId(k, eventStream.S.first), DateTime.now)).run
    saveUser(user2ToSave).run must throwA[Exception] and
      (api1.get(k).run.get must containTheSameElementsAs(List(u1)))
  }

}
