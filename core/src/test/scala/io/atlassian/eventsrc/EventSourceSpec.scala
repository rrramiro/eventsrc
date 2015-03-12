package io.atlassian.eventsrc

import org.junit.runner.RunWith
import org.specs2.SpecificationWithJUnit
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.std.option._
import Transform._

@RunWith(classOf[org.specs2.runner.JUnitRunner])
class EventSourceSpec extends SpecificationWithJUnit {
  import Operation.syntax._
  def is = s2"""
    EventSource.Storage should

      get should return put value                                     $simpleEventStore
      correctly replay a series of commits                            $replayCommits
      correctly replay a series of commits up to a specified sequence $replayToSequence
      correctly store a delete                                        $storesDeletesCorrectly
      conditional insert success                                      $conditionalInsertSuccess
      conditional insert fail                                         $conditionalInsertFail
      conditional delete success                                      $conditionalDeleteSuccess
      conditional delete fail                                         $conditionalDeleteFail
      insert if absent                                                $insertIfAbsent
      delete if absent                                                $deleteIfAbsent
      safe to delete nothing                                          $safeToDeleteNothing
      history should return a list with the history of events         $history
  """

  def simpleEventStore = {
    val source = new MemoryEventSource
    import source.api._

    val testResult = for {
      putResult <- save(1, "fred".insertOp)
      getResult <- get(1)
    } yield getResult

    testResult.run === "fred".some
  }

  def replayCommits = {
    val store = new MemoryEventSource
    import store.api._

    val lotsOfStuff = for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, "barney".insertOp)
      _ <- save(1, "wilma".insertOp)
      get <- get(1)
    } yield get

    lotsOfStuff.run === "wilma".some
  }

  def insertIfAbsent = {
    val store = new MemoryEventSource
    import store.api._
    import scalaz.std.string._
    for {
      _ <- save(1, "fred".insertOp.ifAbsent)
      _ <- save(1, "barney".insertOp.ifAbsent)
      get <- get(1)
    } yield get
  }.run === "fred".some

  def deleteIfAbsent = {
    val store = new MemoryEventSource
    import store.api._
    import scalaz.std.string._
    for {
      del <- save(1, deleteOp)
    } yield del
  }.run === EventSource.Result.Noop()

  def conditionalInsertSuccess = {
    val store = new MemoryEventSource
    import store.api._
    import scalaz.std.string._
    for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, "barney".insertOp.ifEqual("fred"))
      get <- get(1)
    } yield get
  }.run === "barney".some

  def conditionalInsertFail = {
    val store = new MemoryEventSource
    import store.api._
    import scalaz.std.string._
    for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, "barney".insertOp.ifEqual("fred1"))
      get <- get(1)
    } yield get
  }.run === "fred".some

  def conditionalDeleteSuccess = {
    val store = new MemoryEventSource
    import store.api._
    import scalaz.std.string._
    for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, deleteOp.ifEqual("fred"))
      get <- get(1)
    } yield get
  }.run === None

  def conditionalDeleteFail = {
    val store = new MemoryEventSource
    import store.api._
    import scalaz.std.string._
    for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, deleteOp.ifEqual("fred1"))
      get <- get(1)
    } yield get
  }.run === "fred".some

  def replayToSequence = {
    val source = new MemoryEventSource
    import source.api._

    val lotsOfStuff = for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, "barney".insertOp)
      _ <- save(1, "wilma".insertOp)
      get <- getAt(1, 1L)
    } yield get

    lotsOfStuff.run === "barney".some
  }

  def storesDeletesCorrectly = {
    val source = new MemoryEventSource
    import source.api._

    val ops = for {
      _ <- save(1, "fred".insertOp)
      val1 <- get(1)
      _ <- save(1, deleteOp)
      val2 <- get(1)
      val3 <- getAt(1, 0L)
    } yield (val1, val2, val3)

    val expected = ("fred".some, None, "fred".some)
    ops.run === expected
  }

  def safeToDeleteNothing = {
    val source = new MemoryEventSource
    import source.api._

    val ops = for {
      _ <- save(1, deleteOp)
      val1 <- get(1)
    } yield val1
    ops.run === None
  }

  def history = {
    val store = new MemoryEventSource
    import store.api._

    val lotsOfStuff: Task[Process[Task, store.Snapshot]] = for {
      _ <- save(1, "fred".insertOp)
      _ <- save(1, "barney".insertOp)
      _ <- save(1, deleteOp)
      _ <- save(1, "wilma".insertOp)
      snapshot <- getHistory(1)
    } yield snapshot
    val getValue: store.Snapshot => Option[String] = _.value
    val process: Process[Task, store.Snapshot] = lotsOfStuff.run
    val list: List[store.Snapshot] = process.runLog.run.toList
    list.map(getValue) === List(Some("fred"), Some("barney"), None, Some("wilma"))
  }

}
