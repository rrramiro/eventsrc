package io.atlassian.event
package stream

import org.scalacheck.Prop
import org.specs2.execute.AsResult
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.concurrent.Task
import scalaz.syntax.apply._

abstract class DirectoryEventStreamSpec extends ScalaCheckSpec {

  import DirectoryEventStream._

  val defaultMinTests = 100

  def is =
    s2"""
        DirectoryEventStream supports
          Adding multiple users (store list of users) ${addMultipleUsers(defaultMinTests)}
      """

  type ES = EventStorage[Task, DirectoryId, TwoPartSequence[Long], DirectoryEvent]
  type SS = SnapshotStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence[Long], List[User]]

  def getEventStore: Task[ES]
  def getAllUserSnapshot: Task[SS]

  def mkTest(f: (ES, SS) => Prop) = (minTests: Int) =>
    taskTest((getEventStore |@| getAllUserSnapshot)(f(_, _).set(minTestsOk = minTests)))

  val addMultipleUsers = mkTest { (eventStore, _) =>
    Prop.forAll { (k: DirectoryId, u1: User, u2: User) =>
      u1.username != u2.username ==> {
        val api = DirectoryEventStream.allUsersQueryAPIWithNoSnapshots(eventStore)
        val saveApi = DirectoryEventStream.allUsersSaveAPI(api)

        (for {
          _ <- saveApi.save(SaveAPIConfig.default(DefaultExecutor))(k, ops.addUser(u1))
          _ <- saveApi.save(SaveAPIConfig.default(DefaultExecutor))(k, ops.addUser(u2))
          allUsers <- api.get(k, QueryConsistency.LatestEvent)
        } yield allUsers) must returnValue(beSome(containTheSameElementsAs(List(u2, u1))))
      }
    }
  }
}
