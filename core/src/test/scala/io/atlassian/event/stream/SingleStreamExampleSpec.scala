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

         Add and get works $addAndGetClientById
         Add and delete works $addAndDelete
         Add multiple         $addClients
         Delete multiple      $deleteClients
    """

  protected def newEventStream(): ClientEventStream
  protected def newSnapshotStore(): SnapshotStorage[Task, Client.Id, TwoPartSequence, Client.Data]

  def addAndGetClientById = Prop.forAll { (k: Client.Id, c: Client.Data) =>
    val eventStream = newEventStream
    val snapshotStore = newSnapshotStore
    val api = new eventStream.ByKeyQuery(snapshotStore)
    val saveApi = new eventStream.SaveAPI[Client.Id, Client.Data](api)
    (for {
      x <- saveApi.save(k, ClientEvent.insert(k, c).op[TwoPartSequence, Client.Data])
      saved <- api.get(k)
    } yield saved).run.get === c
  }

  def addAndDelete = Prop.forAll { (k: Client.Id, c: Client.Data) =>
    val eventStream = newEventStream
    val snapshotStore = newSnapshotStore
    val api = new eventStream.ByKeyQuery(snapshotStore)
    val saveApi = new eventStream.SaveAPI[Client.Id, Client.Data](api)
    (for {
      _ <- saveApi.save(k, ClientEvent.insert(k, c).op)
      _ <- saveApi.save(k, ClientEvent.delete(k).op)
      saved <- api.get(k)
    } yield saved).run must beNone
  }

  def addClients = Prop.forAll { (c1: Client.Id, d1: Client.Data, c2: Client.Id, d2: Client.Data) =>
    val stream = newEventStream
    val snapshotStore = newSnapshotStore
    val query = new stream.ByKeyQuery(snapshotStore)
    val saveApi = new stream.SaveAPI[Client.Id, Client.Data](query)

    val expected = (Some(d1), Some(d2))
    (for {
      _ <- saveApi.save(c1, Operation.insert(Insert(c1, d1)))
      _ <- saveApi.save(c2, Operation.insert(Insert(c2, d2)))
      read1 <- query.get(c1)
      read2 <- query.get(c2)
    } yield (read1, read2)).run === expected
  }

  def deleteClients = Prop.forAll { (c1: Client.Id, d1: Client.Data, c2: Client.Id, d2: Client.Data) =>
    val stream = newEventStream
    val snapshotStore = newSnapshotStore
    val query = new stream.ByKeyQuery(snapshotStore)
    val saveApi = new stream.SaveAPI[Client.Id, Client.Data](query)

    val expected = (Some(d1), None, Some(d2), None)

    (for {
      _ <- saveApi.save(c1, Operation.insert(Insert(c1, d1)))
      v1 <- query.get(c1)
      _ <- saveApi.save(c1, Operation.insert(Delete(c1)))
      v2 <- query.get(c1)
      _ <- saveApi.save(c2, Operation.insert(Insert(c2, d2)))
      v3 <- query.get(c2)
      _ <- saveApi.save(c2, Operation.insert(Delete(c2)))
      v4 <- query.get(c2)
    } yield (v1, v2, v3, v4)).run === expected
  }

}
