package io.atlassian.event
package stream
package dynamo

import argonaut._, Argonaut._
import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
import kadai.Attempt
import org.scalacheck.{ Arbitrary, Prop }
import org.specs2.main.Arguments
import org.specs2.specification.Step
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }
import scodec.bits.ByteVector

import scalaz._
import scalaz.concurrent.Task

class DynamoDBDirectoryEventStreamSpec(val arguments: Arguments) extends SpecificationWithJUnit with ScalaCheck with LocalDynamoDB with DynamoDBActionMatchers {
  import DirectoryEventStream._
  import Operation.syntax._

  def is =
    s2"""
        DirectoryEventStream supports                               ${Step(startLocalDynamoDB)} ${Step(createTestTable)}
          Adding multiple users (store list of users)               ${addMultipleUsers.set(minTestsOk = NUM_TESTS)}
          Checking for duplicate usernames (store list of users)    ${duplicateUsername.set(minTestsOk = NUM_TESTS)}
          Adding multiple users (store list of users and snapshot) {addMultipleUsersWithSnapshot.set(minTestsOk = NUM_TESTS)}
          Checking for duplicate usernames (store list of users and snapshot) {duplicateUsernameWithSnapshot.set(minTestsOk = NUM_TESTS)}

          Checking for duplicate usernames with sharded store       ${duplicateUsernameSharded.set(minTestsOk = NUM_TESTS)}
          Checking for duplicate usernames with sharded store and snapshot {duplicateUsernameShardedWithSnapshot.set(minTestsOk = NUM_TESTS)}

                                                                    ${Step(deleteTestTable)}
                                                                    ${Step(stopLocalDynamoDB)}
      """
  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  implicit lazy val runner: DynamoDBAction ~> Task =
    new (DynamoDBAction ~> Task) {
      def apply[A](a: DynamoDBAction[A]): Task[A] =
        a.run(DYNAMO_CLIENT).fold({ i => Task.fail(WrappedInvalidException(i)) }, { a => Task.now(a) })
    }

  def createTestTable() =
    DynamoDBOps.createTable(DirectoryEventStreamDynamoMappings.schema)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(DirectoryEventStreamDynamoMappings.schema)

  private def newEventStream = new DynamoDirectoryEventStream(1)

  def addMultipleUsers = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val eventStream = newEventStream
      val api = eventStream.AllUsersQueryAPIWithNoSnapshots
      val saveApi = new eventStream.AllUsersSaveAPI(api)

      (for {
        _ <- saveApi.save(k, ops.addUniqueUser(eventStream)(u1))
        _ <- saveApi.save(k, ops.addUniqueUser(eventStream)(u2))
        allUsers <- api.get(k)
      } yield allUsers).run.get must containTheSameElementsAs(List(u2, u1))
    }
  }

  def duplicateUsername = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = newEventStream
    val api = eventStream.AllUsersQueryAPIWithNoSnapshots
    testDuplicateUsername(eventStream)(api)(k, u1, u2)
  }

  private def testDuplicateUsername(e: DirectoryEventStream)
                                   (queryApi: e.QueryAPI[DirectoryId, List[User]])
                                   (k: DirectoryId, u1: User, u2: User) = {
    val saveApi = new e.AllUsersSaveAPI(queryApi)

    val user2ToSave = u2.copy(username = u1.username)
    (for {
      _ <- saveApi.save(k, ops.addUniqueUser(e)(u1))
      _ <- saveApi.save(k, ops.addUniqueUser(e)(user2ToSave))
      allUsers <- queryApi.get(k)
    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
  }

  def duplicateUsernameSharded = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val events = newEventStream
    val api = new events.ShardedUsernameQueryAPI
    testDuplicateUsernameSharded(events)(api)(k, u1, u2)
  }

  def testDuplicateUsernameSharded(e: DirectoryEventStream)
                                  (queryApi: e.QueryAPI[DirectoryUsername, UserId])
                                  (k: DirectoryId, u1: User, u2: User) = {
    val api1 = new e.AllUsersQueryAPI(DirectoryIdListUserSnapshotStorage)
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
  //
//  def duplicateUsername = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
//    val eventStream = new DirectoryEventStream(1)
//    val api = new eventStream.AllUsersAPI
//
//    val user2ToSave = u2.copy(username = u1.username)
//    (for {
//      _ <- api.save(k, addUniqueUser(eventStream)(u1))
//      _ <- api.save(k, addUniqueUser(eventStream)(user2ToSave))
//      allUsers <- api.get(k)
//    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
//  }
//
//  def addMultipleUsersWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
//    u1.username != u2.username ==> {
//      val eventStream = new DirectoryEventStream(1)
//      val api = new eventStream.AllUsersAPI
//
//      val (allUsers, snapshotUser1, snapshotUser2) = (for {
//        _ <- api.save(k, addUniqueUser(eventStream)(u1))
//        snapshotUser1 <- api.refreshSnapshot(k, None)
//        savedUser2 <- api.save(k, addUniqueUser(eventStream)(u2))
//        seq = savedUser2.fold(_.seq, _ => None, None)
//        snapshotUser2 <- api.refreshSnapshot(k, None)
//        allUsers <- api.get(k)
//      } yield (allUsers, snapshotUser1, snapshotUser2)).run
//
//      allUsers.get must containTheSameElementsAs(List(u2, u1)) and
//        (snapshotUser1.toOption.get.value === Some(List(u1))) and
//        (snapshotUser2.toOption.get.value.get must containTheSameElementsAs(List(u2, u1)))
//    }
//  }
//
//  def duplicateUsernameWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
//    val eventStream = new DirectoryEventStream(1)
//    val api = new eventStream.AllUsersAPI
//
//    val user2ToSave = u2.copy(username = u1.username)
//    (for {
//      _ <- api.save(k, addUniqueUser(eventStream)(u1))
//      _ <- api.snapshotStore.put(k, Snapshot.value(List(u1), eventStream.S.first, DateTime.now))
//      _ <- api.save(k, addUniqueUser(eventStream)(user2ToSave))
//      allUsers <- api.get(k)
//    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
//  }
//
//  def duplicateUsernameSharded = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
//    val events = new DirectoryEventStream(1)
//    val api1 = new events.AllUsersAPI
//    val api2 = new events.ShardedUsernameAPI
//
//    val user2ToSave = u2.copy(username = u1.username)
//
//    // Do the saving operation. This is kind of ugly but supports multiple conditions
//    def saveUser(u: User) =
//      for {
//        s <- api2.getSnapshot((k, u.username))
//        op <- s.fold(
//          Task.now(Operation[api2.K, events.S, api2.V, events.E] { _ => Result.success[events.E](AddUser(u)) }),
//          (_, _, _) => Task.fail(new Exception("Duplicate username")),
//          (id, _) => Task.now {
//            Operation[api2.K, events.S, api2.V, events.E] {
//              s2 =>
//                if (s.seq == s2.seq) Result.success[events.E](AddUser(u))
//                else Result.reject(Reason("Locking exception").wrapNel)
//            }
//          }
//        )
//        _ <- api2.save((k, u.username), op)
//      } yield ()
//
//    saveUser(u1).attemptRun
//    saveUser(user2ToSave).run must throwA[Exception] and
//      (api1.get(k).run.get must containTheSameElementsAs(List(u1)))
//  }
//
//  def duplicateUsernameShardedWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
//    val events = new DirectoryEventStream(1)
//    val api1 = new events.AllUsersAPI
//    val api2 = new events.ShardedUsernameAPI
//
//    val user2ToSave = u2.copy(username = u1.username)
//
//    // Do the saving operation. This is kind of ugly but supports multiple conditions
//    def saveUser(u: User) =
//      for {
//        s <- api2.getSnapshot((k, u.username))
//        op <- s.fold(
//          Task.now(Operation[api2.K, events.S, api2.V, events.E] { _ => Result.success(AddUser(u)) }),
//          (_, _, _) => Task.fail(new Exception("Duplicate username")),
//          (id, _) => Task.now {
//            Operation[api2.K, events.S, api2.V, events.E] {
//              s2 =>
//                if (s.seq == s2.seq) Result.success(AddUser(u))
//                else Result.reject(Reason("Locking exception").wrapNel)
//            }
//          }
//        )
//        _ <- api2.save((k, u.username), op)
//      } yield ()
//
//    saveUser(u1).attemptRun
//    // Manually save a snapshot
//    api2.snapshotStore.put((k, u1.username), Snapshot.value(u1.id, events.S.first, DateTime.now)).run
//    saveUser(user2ToSave).run must throwA[Exception] and
//      (api1.get(k).run.get must containTheSameElementsAs(List(u1)))
//  }

}

import DirectoryEventStream._

object DirectoryEventStreamDynamoMappings {
  implicit val TwoPartSeqEncoder: Encoder[TwoPartSequence] =
    Encoder[NonEmptyBytes].contramap { seq =>
      ByteVector.fromLong(seq.seq) ++ ByteVector.fromLong(seq.zone) match {
        case NonEmptyBytes(b) => b
      }
    }

  implicit val TwoPartSeqDecoder: Decoder[TwoPartSequence] =
    Decoder[NonEmptyBytes].mapAttempt { bytes =>
      if (bytes.bytes.length != 16)
        Attempt.fail(s"Invalid length of byte vector ${bytes.bytes.length}")
      else
        Attempt.safe {
          bytes.bytes.splitAt(8) match {
            case (abytes, bbytes) => TwoPartSequence(abytes.toLong(), bbytes.toLong())
          }
        }
    }

  implicit val DirectoryIdEncoder: Encoder[DirectoryId] =
    Encoder[String].contramap { _.id }

  implicit val DirectoryIdDecoder: Decoder[DirectoryId] =
    Decoder[String].map { DirectoryId.apply }

  implicit val DirectoryEventEncodeJson: EncodeJson[DirectoryEvent] =
    EncodeJson {
      case a @ AddUser(u) => ("add-user" := u) ->: jEmptyObject
    }

  private[dynamo] val AddUserDecodeJson: DecodeJson[AddUser] =
    DecodeJson { c =>
      for {
        u <- c.get[User]("add-user")
      } yield AddUser(u)
    }

  implicit val DirectoryEventDecodeJson: DecodeJson[DirectoryEvent] =
    AddUserDecodeJson map identity

  val key = Column[DirectoryId]("key")
  val seq = Column[TwoPartSequence]("seq")
  val event = Column[DirectoryEvent]("event")
  val tableName = s"dynamo_dir_event_stream_test_${System.currentTimeMillis}"
  val schema = TableDefinition.from[DirectoryId, DirectoryEvent, DirectoryId, TwoPartSequence](tableName, key, event, key, seq)
}

class DynamoDirectoryEventStream(zone: ZoneId)(implicit runner: DynamoDBAction ~> Task) extends DirectoryEventStream(zone) {
  import DirectoryEventStreamDynamoMappings._

  implicit val TaskToTask = NaturalTransformation.refl[Task]

  val eventStore = new DynamoEventStorage[Task, KK, S, E](schema)

  //  class ShardedUsernameAPI extends API[DirectoryUsername, UserId] {
//    override def eventStreamKey = _._1
//
//    override def acc(key: DirectoryUsername)(v: Snapshot[DirectoryUsername, S, UserId], e: Event[KK, S, E]): Snapshot[DirectoryUsername, S, UserId] =
//      e.operation match {
//        case AddUser(user) =>
//          if (key._2 == user.username)
//            Snapshot.value(user.id, e.id.seq, e.time)
//          else
//            v
//      }
//
//    object snapshotStore extends SnapshotStorage[Task, DirectoryUsername, S, UserId] {
//      val map = collection.concurrent.TrieMap[DirectoryUsernamePrefix, Snapshot[DirectoryUsername, S, Map[Username, UserId]]]()
//      def get(key: DirectoryUsername, seq: SequenceQuery[TwoPartSequence]): Task[Snapshot[DirectoryUsername, S, UserId]] =
//        Task {
//          map.getOrElse(prefix(key), Snapshot.zero).fold(Snapshot.zero[DirectoryUsername, S, UserId],
//            (m, id, t) =>
//              m.get(key._2).fold(Snapshot.deleted[DirectoryUsername, S, UserId](id, t)) { uid => Snapshot.value(uid, id, t) },
//            (id, t) => Snapshot.deleted(id, t)) // This should not happen
//        }
//
//      def put(key: DirectoryUsername, view: Snapshot[DirectoryUsername, S, UserId]): Task[SnapshotStorage.Error \/ Snapshot[DirectoryUsername, S, UserId]] =
//        Task {
//          map.get(prefix(key)) match {
//            case None =>
//              val newSnapshot: Snapshot[DirectoryUsername, S, Map[Username, UserId]] =
//                view.fold(Snapshot.zero[DirectoryUsername, S, Map[Username, UserId]], (uid, id, t) => Snapshot.value(Map(key._2 -> uid), id, t), (id, t) => Snapshot.value(Map(), id, t))
//              map += (prefix(key) -> newSnapshot)
//            case Some(s) =>
//              (s, view) match {
//                case (Snapshot.Value(m, id1, t1), Snapshot.Value(uid, id2, t2)) =>
//                  map += (prefix(key) -> Snapshot.Value(m + (key._2 -> uid), id2, t2))
//                case (Snapshot.Value(m, id1, t1), Snapshot.Deleted(id2, t2)) =>
//                  map += (prefix(key) -> Snapshot.Value(m - key._2, id2, t2))
//                case _ =>
//                  ()
//              }
//          }
//          view.right
//        }
//
//      private def prefix(key: DirectoryUsername): (DirectoryId, String) =
//        (key._1, key._2.substring(0, Math.min(3, key._2.length)))
//    }
//  }
//
//  class AllUsersAPI extends API[DirectoryId, List[User]] {
//    override def eventStreamKey = k => k
//
//    override def acc(key: DirectoryId)(v: Snapshot[DirectoryId, S, List[User]], e: Event[KK, S, E]): Snapshot[DirectoryId, S, List[User]] =
//      e.operation match {
//        case AddUser(user) =>
//          val userList: List[User] =
//            v.value.fold(List(user)) { l => user :: l }
//
//          Snapshot.Value(userList, e.id.seq, e.time)
//      }
//
//    object snapshotStore extends SnapshotStorage[Task, DirectoryId, S, List[User]] {
//      val map = collection.concurrent.TrieMap[DirectoryId, Snapshot[DirectoryId, S, List[User]]]()
//
//      def get(key: DirectoryId, sequence: SequenceQuery[TwoPartSequence]): Task[Snapshot[DirectoryId, S, List[User]]] =
//        Task {
//          map.getOrElse(key, Snapshot.zero)
//        }
//
//      def put(key: DirectoryId, view: Snapshot[DirectoryId, S, List[User]]): Task[SnapshotStorage.Error \/ Snapshot[DirectoryId, S, List[User]]] =
//        Task {
//          map += (key -> view)
//          view.right
//        }
//    }
//  }
}

// TODO - snapshot storage for DirectoryId -> List[User]
//  Q)