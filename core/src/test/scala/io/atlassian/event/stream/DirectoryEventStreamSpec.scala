package io.atlassian.event
package stream

import io.atlassian.event.stream.EventStream.QueryConsistency
import org.scalacheck.Prop
import org.specs2.{ScalaCheck, SpecificationWithJUnit}

import scalaz.concurrent.Task

abstract class DirectoryEventStreamSpec extends SpecificationWithJUnit with ScalaCheck {

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

  protected def newEventStream(): DirectoryEventStream
  protected def allUserSnapshot(): SnapshotStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence, List[User]]

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

  def duplicateUsernameWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val eventStream = newEventStream
    val api = new eventStream.AllUsersQueryAPIWithSnapshotPersistence(allUserSnapshot)
    testDuplicateUsername(eventStream)(api)(k, u1, u2)
  }

  def addMultipleUsersWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val eventStream = newEventStream
      val api = new eventStream.AllUsersQueryAPIWithSnapshotPersistence(allUserSnapshot)
      val saveApi = new eventStream.AllUsersSaveAPI(api)

      val (allUsers, snapshotUser1, snapshotUser2) = (for {
        _ <- saveApi.save(k, ops.addUniqueUser(eventStream)(u1))
        snapshotUser1 <- api.get(k, QueryConsistency.LatestSnapshot)
        savedUser2 <- saveApi.save(k, ops.addUniqueUser(eventStream)(u2))
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

  def duplicateUsernameShardedWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val events = newEventStream
    val api = new events.ShardedUsernameQueryAPIWithSnapshotPersistence
    testDuplicateUsernameSharded(events)(api)(k, u1, u2)
  }

  def testDuplicateUsernameSharded(e: DirectoryEventStream)
                                  (queryApi: e.QueryAPI[DirectoryUsername, UserId])
                                  (k: DirectoryId, u1: User, u2: User) = {
    val api1 = new e.AllUsersQueryAPI(allUserSnapshot)
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
}
