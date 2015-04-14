package io.atlassian.event
package stream

import io.atlassian.event.stream.EventStream.QueryConsistency
import org.scalacheck.{ Gen, Arbitrary, Prop }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.\/
import scalaz.concurrent.Task
import scalaz.syntax.either._
import Arbitrary.arbitrary

class DirectoryEventStreamSpec extends SpecificationWithJUnit with ScalaCheck {

  import DirectoryEventStream._
  import Operation.syntax._

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
      val api = new eventStream.AllUsersQueryAPI
      val saveApi = new eventStream.AllUsersSaveAPI(api)

      (for {
        _ <- saveApi.save(k, addUniqueUser(eventStream)(u1))
        _ <- saveApi.save(k, addUniqueUser(eventStream)(u2))
        allUsers <- api.get(k)
      } yield allUsers).run.get must containTheSameElementsAs(List(u2, u1))
    }
  }

  def duplicateUsername = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = new DirectoryEventStream(1)
    val api = new eventStream.AllUsersQueryAPI
    testDuplicateUsername(eventStream)(api)(k, u1, u2)
  }

  def duplicateUsernameWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = new DirectoryEventStream(1)
    val api = new eventStream.AllUsersQueryAPIWithSnapshotPersistence
    testDuplicateUsername(eventStream)(api)(k, u1, u2)
  }

  def addMultipleUsersWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val eventStream = new DirectoryEventStream(1)
      val api = new eventStream.AllUsersQueryAPIWithSnapshotPersistence
      val saveApi = new eventStream.AllUsersSaveAPI(api)

      val (allUsers, snapshotUser1, snapshotUser2) = (for {
        _ <- saveApi.save(k, addUniqueUser(eventStream)(u1))
        snapshotUser1 <- api.get(k, QueryConsistency.LatestSnapshot)
        savedUser2 <- saveApi.save(k, addUniqueUser(eventStream)(u2))
        seq = savedUser2.fold(_.seq, _ => None, None)
        snapshotUser2 <- api.get(k, QueryConsistency.LatestSnapshot)
        allUsers <- api.get(k)
      } yield (allUsers, snapshotUser1, snapshotUser2)).run

      allUsers.get must containTheSameElementsAs(List(u2, u1)) and
        (snapshotUser1 === Some(List(u1))) and
        (snapshotUser2.get must containTheSameElementsAs(List(u2, u1)))
    }
  }

  private def testDuplicateUsername(e: DirectoryEventStream)
                                   (queryApi: e.QueryAPI[DirectoryId, List[User]])
                                   (k: DirectoryId, u1: User, u2: User) = {
    val saveApi = new e.AllUsersSaveAPI(queryApi)

    val user2ToSave = u2.copy(username = u1.username)
    (for {
      _ <- saveApi.save(k, addUniqueUser(e)(u1))
      _ <- saveApi.save(k, addUniqueUser(e)(user2ToSave))
      allUsers <- queryApi.get(k)
    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
  }

  def duplicateUsernameSharded = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val events = new DirectoryEventStream(1)
    val api = new events.ShardedUsernameQueryAPI
    testDuplicateUsernameSharded(events)(api)(k, u1, u2)
  }

  def duplicateUsernameShardedWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val events = new DirectoryEventStream(1)
    val api = new events.ShardedUsernameQueryAPIWithSnapshotPersistence
    testDuplicateUsernameSharded(events)(api)(k, u1, u2)
  }

  def testDuplicateUsernameSharded(e: DirectoryEventStream)
                                  (queryApi: e.QueryAPI[DirectoryUsername, UserId])
                                  (k: DirectoryId, u1: User, u2: User) = {
    val api1 = new e.AllUsersQueryAPI
    val saveApi = new e.SaveAPI[DirectoryUsername, UserId](queryApi)

    val user2ToSave = u2.copy(username = u1.username)

    // Do the saving operation. This is kind of ugly but supports multiple conditions
    def saveUser(u: User) =
      for {
        s <- queryApi.getSnapshot((k, u.username))
        op <- s.fold(
          Task.now(Operation.insert[queryApi.K, e.S, queryApi.V, e.E](AddUser(u))),
          (_, _, _) => Task.fail(new Exception("Duplicate username")),
          (id, _) => Task.now {
            Operation.ifSeq[queryApi.K, e.S, queryApi.V, e.E](id, AddUser(u))
          }
        )
        _ <- saveApi.save((k, u.username), op)
      } yield ()

    saveUser(u1).attemptRun
    saveUser(user2ToSave).run must throwA[Exception] and
      (api1.get(k).run.get must containTheSameElementsAs(List(u1)))
  }

  import scalaz.syntax.std.option._
  import DataValidator._

  def addUniqueUser(events: DirectoryEventStream)(u: User): Operation[DirectoryId, events.S, List[User], events.E] =
    DirectoryEvent.addUser(u).op[DirectoryId, events.S, List[User]].filter { noDuplicateUsername(u) }

  private def noDuplicateUsername(u: User): DataValidator.Validator[List[User]] =
    ol =>
      if ((ol | Nil).exists { _.username == u.username })
        "User with same username already exists".fail
      else
        DataValidator.success
}

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
object DirectoryEvent {
  def addUser(user: User): DirectoryEvent =
    AddUser(user)
}

import DirectoryEventStream._

case class User(id: UserId, username: Username)

class DirectoryEventStream(zone: ZoneId) extends EventStream[Task] {

  type KK = DirectoryId
  type S = TwoPartSequence
  type E = DirectoryEvent

  override implicit lazy val S = TwoPartSequence.twoPartSequence(zone)

  val eventStore = new MemoryEventStorage[KK, S, E]

  class ShardedUsernameQueryAPI extends QueryAPI[DirectoryUsername, UserId] {
    override def eventStreamKey = _._1

    override def acc(key: DirectoryUsername)(v: Snapshot[DirectoryUsername, S, UserId], e: Event[KK, S, E]): Snapshot[DirectoryUsername, S, UserId] =
      e.operation match {
        case AddUser(user) =>
          if (key._2 == user.username)
            Snapshot.value(user.id, e.id.seq, e.time)
          else
            v
      }

    object snapshotStore extends SnapshotStorage[Task, DirectoryUsername, S, UserId] {
      val map = collection.concurrent.TrieMap[DirectoryUsernamePrefix, Snapshot[DirectoryUsername, S, Map[Username, UserId]]]()
      def get(key: DirectoryUsername, seq: SequenceQuery[TwoPartSequence]): Task[Snapshot[DirectoryUsername, S, UserId]] =
        Task {
          map.getOrElse(prefix(key), Snapshot.zero).fold(Snapshot.zero[DirectoryUsername, S, UserId],
            (m, id, t) =>
              m.get(key._2).fold(Snapshot.deleted[DirectoryUsername, S, UserId](id, t)) { uid => Snapshot.value(uid, id, t) },
            (id, t) => Snapshot.deleted(id, t)) // This should not happen
        }

      def put(key: DirectoryUsername, view: Snapshot[DirectoryUsername, S, UserId]): Task[SnapshotStorage.Error \/ Snapshot[DirectoryUsername, S, UserId]] =
        Task {
          map.get(prefix(key)) match {
            case None =>
              val newSnapshot: Snapshot[DirectoryUsername, S, Map[Username, UserId]] =
                view.fold(Snapshot.zero[DirectoryUsername, S, Map[Username, UserId]], (uid, id, t) => Snapshot.value(Map(key._2 -> uid), id, t), (id, t) => Snapshot.value(Map(), id, t))
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
  }

  class ShardedUsernameQueryAPIWithSnapshotPersistence extends ShardedUsernameQueryAPI {
    override val runPersistSnapshot: Task[SnapshotStorage.Error \/ Snapshot[K, S, V]] => Unit =
      _.run.fold(println(_), _ => ())
  }

  class AllUsersQueryAPI extends QueryAPI[DirectoryId, List[User]] {
    override def eventStreamKey = k => k

    override def acc(key: DirectoryId)(v: Snapshot[DirectoryId, S, List[User]], e: Event[KK, S, E]): Snapshot[DirectoryId, S, List[User]] =
      e.operation match {
        case AddUser(user) =>
          val userList: List[User] =
            v.value.fold(List(user)) { l => user :: l }

          Snapshot.Value(userList, e.id.seq, e.time)
      }

    val snapshotStore = DirectoryIdListUserSnapshotStorage
  }

  class AllUsersQueryAPIWithSnapshotPersistence extends AllUsersQueryAPI {
    override val runPersistSnapshot: Task[SnapshotStorage.Error \/ Snapshot[K, S, V]] => Unit =
      _.run.fold(e => throw new RuntimeException(e.toString), _ => ())
  }

  object DirectoryIdListUserSnapshotStorage extends SnapshotStorage[Task, DirectoryId, S, List[User]] {
    val map = collection.concurrent.TrieMap[DirectoryId, Snapshot[DirectoryId, S, List[User]]]()

    def get(key: DirectoryId, sequence: SequenceQuery[TwoPartSequence]): Task[Snapshot[DirectoryId, S, List[User]]] =
      Task {
        map.getOrElse(key, Snapshot.zero)
      }

    def put(key: DirectoryId, view: Snapshot[DirectoryId, S, List[User]]): Task[SnapshotStorage.Error \/ Snapshot[DirectoryId, S, List[User]]] =
      Task {
        map += (key -> view)
        view.right
      }
  }

  class AllUsersSaveAPI(query: QueryAPI[DirectoryId, List[User]]) extends SaveAPI[DirectoryId, List[User]](query)
}
