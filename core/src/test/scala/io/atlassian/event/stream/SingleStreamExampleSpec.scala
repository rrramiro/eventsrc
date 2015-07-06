package io.atlassian.event
package stream

import org.scalacheck.Prop

import scalaz.concurrent.{ Strategy, Task }
import Operation.syntax._

abstract class SingleStreamExampleSpec extends ScalaCheckSpec {
  import SingleStreamExample._

  def is =
    s2"""
         This spec tests a single event stream case

         Add and get works    $addAndGetClientById
         Add and delete works $addAndDelete
         Add multiple         $addClients
         Delete multiple      $deleteClients
    """

  protected def eventStore: EventStorage[Task, SingleStreamKey, TwoPartSequence[Long], ClientEvent]
  protected def snapshotStore: SnapshotStorage[Task, Client.Id, TwoPartSequence[Long], Client.Data]

  private[this] def query = SingleStreamExample.clientEventStream(eventStore, snapshotStore)
  private[this] def saveApi = DirectoryEventStream.allUsersSaveAPI(query)

  def addAndGetClientById = Prop.forAll { (k: Client.Id, c: Client.Data) =>
    (for {
      x <- saveApi.save(k, ClientEvent.insert(k, c).op[TwoPartSequence[Long], Client.Data])(SaveAPIConfig.default)
      saved <- query.get(k, QueryConsistency.LatestEvent)
    } yield saved).run.get === c
  }

  def addAndDelete = Prop.forAll { (k: Client.Id, c: Client.Data) =>
    (for {
      _ <- saveApi.save(k, ClientEvent.insert(k, c).op)(SaveAPIConfig.default)
      _ <- saveApi.save(k, ClientEvent.delete(k).op)(SaveAPIConfig.default)
      saved <- query.get(k, QueryConsistency.LatestEvent)
    } yield saved).run must beNone
  }

  def addClients = Prop.forAll { (c1: Client.Id, d1: Client.Data, c2: Client.Id, d2: Client.Data) =>
    val expected = (Some(d1), Some(d2))
    (for {
      _ <- saveApi.save(c1, Operation.insert(Insert(c1, d1)))(SaveAPIConfig.default)
      _ <- saveApi.save(c2, Operation.insert(Insert(c2, d2)))(SaveAPIConfig.default)
      read1 <- query.get(c1, QueryConsistency.LatestEvent)
      read2 <- query.get(c2, QueryConsistency.LatestEvent)
    } yield (read1, read2)).run === expected
  }

  def deleteClients = Prop.forAll { (c1: Client.Id, d1: Client.Data, c2: Client.Id, d2: Client.Data) =>
    val expected = (Some(d1), None, Some(d2), None)
    (for {
      _ <- saveApi.save(c1, Operation.insert(Insert(c1, d1)))(SaveAPIConfig.default)
      v1 <- query.get(c1, QueryConsistency.LatestEvent)
      _ <- saveApi.save(c1, Operation.insert(Delete(c1)))(SaveAPIConfig.default)
      v2 <- query.get(c1, QueryConsistency.LatestEvent)
      _ <- saveApi.save(c2, Operation.insert(Insert(c2, d2)))(SaveAPIConfig.default)
      v3 <- query.get(c2, QueryConsistency.LatestEvent)
      _ <- saveApi.save(c2, Operation.insert(Delete(c2)))(SaveAPIConfig.default)
      v4 <- query.get(c2, QueryConsistency.LatestEvent)
    } yield (v1, v2, v3, v4)).run === expected
  }

}
