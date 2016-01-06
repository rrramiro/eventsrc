package io.atlassian.event
package stream

import org.scalacheck.Prop
import org.specs2.execute.AsResult

import scalaz.concurrent.{ Strategy, Task }
import Operation.syntax._

abstract class SingleStreamExampleSpec extends ScalaCheckSpec {
  import SingleStreamExample._

  private[this] val NUM_TESTS = 100

  def is =
    s2"""
         This spec tests a single event stream case

         Add and get works    ${addAndGetClientById(NUM_TESTS)}
         Add and delete works ${addAndDelete(NUM_TESTS)}
         Add multiple         ${addClients(NUM_TESTS)}
         Delete multiple      ${deleteClients(NUM_TESTS)}
    """

  type Q = QueryAPI[Task, SingleStreamKey, ClientEvent, Client.Id, TwoPartSequence[Long], Client.Data]
  type S = SaveAPI[Task, SingleStreamKey, ClientEvent, Client.Id, TwoPartSequence[Long]]

  type ES = EventStorage[Task, SingleStreamKey, TwoPartSequence[Long], ClientEvent]
  type SS = SnapshotStorage[Task, Client.Id, TwoPartSequence[Long], Client.Data]

  def getEventStore: Task[ES]
  def getSnapshotStore: Task[SS]

  def mkTest(f: (Q, S) => Prop) = (minTests: Int) =>
    taskTest {
      for {
        eventStore <- getEventStore
        snapshotStore <- getSnapshotStore
        query = SingleStreamExample.clientEventStream(eventStore, snapshotStore)
        save = DirectoryEventStream.allUsersSaveAPI(query)
      } yield f(query, save).set(minTestsOk = minTests)
    }

  val addAndGetClientById = mkTest { (q, s) =>
    Prop.forAll { (k: Client.Id, c: Client.Data) =>
      (for {
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(k, ClientEvent.insert(k, c).op[TwoPartSequence[Long]])
        saved <- q.get(k, QueryConsistency.LatestEvent)
      } yield saved) must returnValue(beSome(c))
    }
  }

  val addAndDelete = mkTest { (q, s) =>
    Prop.forAll { (k: Client.Id, c: Client.Data) =>
      (for {
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(k, ClientEvent.insert(k, c).op)
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(k, ClientEvent.delete(k).op)
        saved <- q.get(k, QueryConsistency.LatestEvent)
      } yield saved) must returnValue(beNone)
    }
  }

  def addClients = mkTest { (q, s) =>
    Prop.forAll { (c1: Client.Id, d1: Client.Data, c2: Client.Id, d2: Client.Data) =>
      val expected = (Some(d1), Some(d2))
      (for {
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(c1, Operation.insert(Insert(c1, d1)))
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(c2, Operation.insert(Insert(c2, d2)))
        read1 <- q.get(c1, QueryConsistency.LatestEvent)
        read2 <- q.get(c2, QueryConsistency.LatestEvent)
      } yield (read1, read2)) must returnValue(expected)
    }
  }

  def deleteClients = mkTest { (q, s) =>
    Prop.forAll { (c1: Client.Id, d1: Client.Data, c2: Client.Id, d2: Client.Data) =>
      val expected = (Some(d1), None, Some(d2), None)
      (for {
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(c1, Operation.insert(Insert(c1, d1)))
        v1 <- q.get(c1, QueryConsistency.LatestEvent)
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(c1, Operation.insert(Delete(c1)))
        v2 <- q.get(c1, QueryConsistency.LatestEvent)
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(c2, Operation.insert(Insert(c2, d2)))
        v3 <- q.get(c2, QueryConsistency.LatestEvent)
        _ <- s.save(SaveAPIConfig.default(DefaultExecutor))(c2, Operation.insert(Delete(c2)))
        v4 <- q.get(c2, QueryConsistency.LatestEvent)
      } yield (v1, v2, v3, v4)) must returnValue(expected)
    }
  }
}
