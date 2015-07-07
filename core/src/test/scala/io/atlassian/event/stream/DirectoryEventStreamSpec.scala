package io.atlassian.event
package stream

import org.scalacheck.Prop
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.concurrent.Task

abstract class DirectoryEventStreamSpec extends SpecificationWithJUnit with ScalaCheck {

  import DirectoryEventStream._

  def is =
    s2"""
        DirectoryEventStream supports
          Adding multiple users (store list of users) $addMultipleUsers
          Checking for duplicate usernames (store list of users) $duplicateUsername
          Adding multiple users (store list of users and snapshot) $addMultipleUsersWithSnapshot

          Checking for duplicate usernames with sharded store $duplicateUsernameSharded

      """

  protected def eventStore: EventStorage[Task, DirectoryId, TwoPartSequence[Long], DirectoryEvent]
  protected def allUserSnapshot(): SnapshotStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence[Long], List[User]]

  val addMultipleUsers = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val api = DirectoryEventStream.allUsersQueryAPIWithNoSnapshots(eventStore)
      val saveApi = DirectoryEventStream.allUsersSaveAPI(api)

      (for {
        _ <- saveApi.save(k, ops.addUniqueUser(u1))(SaveAPIConfig.default)
        _ <- saveApi.save(k, ops.addUniqueUser(u2))(SaveAPIConfig.default)
        allUsers <- api.get(k, QueryConsistency.LatestEvent)
      } yield allUsers).run.get must containTheSameElementsAs(List(u2, u1))
    }
  }

  val duplicateUsername = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val api = DirectoryEventStream.allUsersQueryAPIWithNoSnapshots(eventStore)
    testDuplicateUsername(api)(k, u1, u2)
  }

  val addMultipleUsersWithSnapshot = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    u1.username != u2.username ==> {
      val api = DirectoryEventStream.allUsersQueryAPI(eventStore, allUserSnapshot)
      val saveApi = DirectoryEventStream.allUsersSaveAPI(api)

      val (allUsers, snapshotUser1, snapshotUser2) = (for {
        _ <- saveApi.save(k, ops.addUniqueUser(u1))(SaveAPIConfig.default)
        snapshotUser1 <- api.get(k, QueryConsistency.LatestSnapshot)
        savedUser2 <- saveApi.save(k, ops.addUniqueUser(u2))(SaveAPIConfig.default)
        seq = savedUser2.fold(_.seq, _ => None, _ => None)
        snapshotUser2 <- api.get(k, QueryConsistency.LatestSnapshot)
        allUsers <- api.get(k, QueryConsistency.LatestEvent)
      } yield (allUsers, snapshotUser1, snapshotUser2)).run

      allUsers.get must containTheSameElementsAs(List(u2, u1)) and
        (snapshotUser1 === Some(List(u1))) and
        (snapshotUser2.get must containTheSameElementsAs(List(u2, u1)))
    }
  }

  private def testDuplicateUsername(queryApi: QueryAPI[Task, DirectoryId, DirectoryEvent, DirectoryId, TwoPartSequence[Long], List[User]])(k: DirectoryId, u1: User, u2: User) = {
    val saveApi = DirectoryEventStream.allUsersSaveAPI(queryApi)

    val user2ToSave = u2.copy(username = u1.username)
    (for {
      _ <- saveApi.save(k, ops.addUniqueUser(u1))(SaveAPIConfig.default)
      _ <- saveApi.save(k, ops.addUniqueUser(user2ToSave))(SaveAPIConfig.default)
      allUsers <- queryApi.get(k, QueryConsistency.LatestEvent)
    } yield allUsers).run.get must containTheSameElementsAs(List(u1))
  }

  def duplicateUsernameSharded = Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
    val api = DirectoryEventStream.shardedUsernameQueryAPI(eventStore)
    testDuplicateUsernameSharded(api)(k, u1, u2)
  }

  def testDuplicateUsernameSharded(queryApi: QueryAPI[Task, DirectoryId, DirectoryEvent, DirectoryUsername, TwoPartSequence[Long], UserId])(k: DirectoryId, u1: User, u2: User) = {
    val api1 = DirectoryEventStream.allUsersQueryAPI(eventStore, allUserSnapshot)
    val saveApi = DirectoryEventStream.allUsersSaveAPI(queryApi)

    val user2ToSave = u2.copy(username = u1.username)

    // Do the saving operation. This is kind of ugly but supports multiple conditions
    def saveUser(u: User) =
      for {
        s <- queryApi.getSnapshot((k, u.username), QueryConsistency.LatestEvent)
        op <- s.fold(
          Task.now(Operation.insert[TwoPartSequence[Long], UserId, DirectoryEvent](AddUser(u))),
          (_, _, _) => Task.fail(new Exception("Duplicate username")),
          (id, _) => Task.now {
            Operation.ifSeq[TwoPartSequence[Long], UserId, DirectoryEvent](id, AddUser(u))
          }
        )
        _ <- saveApi.save((k, u.username), op)(SaveAPIConfig.default)
      } yield ()

    saveUser(u1).attemptRun
    saveUser(user2ToSave).run must throwA[Exception] and
      (api1.get(k, QueryConsistency.LatestEvent).run.get must containTheSameElementsAs(List(u1)))
  }
}
