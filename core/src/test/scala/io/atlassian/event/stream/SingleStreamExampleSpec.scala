package io.atlassian.event
package stream

import org.scalacheck.Prop

import scalaz.concurrent.Task
import Operation.syntax._

abstract class SingleStreamExampleSpec extends ScalaCheckSpec {
  import SingleStreamExample._

  def is =
    s2"""
         This spec tests a single event stream case

         Add and get works $addAndGetClientById
         Add and delete works $addAndDelete
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

}
