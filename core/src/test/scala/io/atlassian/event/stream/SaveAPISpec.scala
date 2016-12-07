package io.atlassian.event.stream

import io.atlassian.event.{ ScalaCheckSpec, RetryInterval, RetryStrategy, TwoPartSequence }
import io.atlassian.event.stream.memory.{ MemoryEventStorage, MemorySingleSnapshotStorage }

import scala.concurrent.duration._

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen.posNum
import org.scalacheck.Prop

import scalaz.concurrent.Task
import scalaz.effect.IO
import scalaz.stream.Process
import scalaz.syntax.applicative._
import scalaz.{ \/, NonEmptyList, OptionT, Traverse }

class SaveAPISpec extends ScalaCheckSpec {
  import SingleStreamExample._

  def is =
    s2"""
         Specification of operations on SaveAPI

         Retries on batch will retry the whole batch      ${batchRetries}
    """

  type Q = QueryAPI[Task, SingleStreamKey, ClientEvent, Client.Id, TwoPartSequence[Long], Client.Data]
  type S = SaveAPI[Task, SingleStreamKey, ClientEvent, Client.Id, TwoPartSequence[Long]]

  type ES = EventStorage[Task, SingleStreamKey, TwoPartSequence[Long], ClientEvent]
  type SS = SnapshotStorage[Task, Client.Id, TwoPartSequence[Long], Client.Data]

  val getEventStore: Task[ES] =
    MemoryEventStorage[SingleStreamKey, TwoPartSequence[Long], ClientEvent]
  val getSnapshotStore: Task[SS] =
    MemorySingleSnapshotStorage[Client.Id, TwoPartSequence[Long], Client.Data]

  val maxBatchSize: Int =
    50

  def instantRetryStrategy(count: Int): RetryStrategy[Task] =
    RetryStrategy.retryIntervals[Task](
      RetryInterval.fullJitter(count, 1.millis, 1.0), _ => ().point[IO]
    )

  def getStores(retryCount: Int): Task[(ES, S)] =
    for {
      es <- MemoryEventStorage[SingleStreamKey, TwoPartSequence[Long], ClientEvent]
      eventStore = new ES {
        def batchPut[G[_]: Traverse](events: G[Event[SingleStreamKey, TwoPartSequence[Long], ClientEvent]]): Task[EventStreamError \/ G[Event[SingleStreamKey, TwoPartSequence[Long], ClientEvent]]] =
          es.batchPut(events).as(\/.left(EventStreamError.DuplicateEvent))

        def get(key: SingleStreamKey, fromSeq: Option[TwoPartSequence[Long]]): Process[Task, Event[SingleStreamKey, TwoPartSequence[Long], ClientEvent]] =
          es.get(key, fromSeq)

        def latest(key: SingleStreamKey): OptionT[Task, Event[SingleStreamKey, TwoPartSequence[Long], ClientEvent]] =
          es.latest(key)

        def put(event: Event[SingleStreamKey, TwoPartSequence[Long], ClientEvent]): Task[EventStreamError \/ Event[SingleStreamKey, TwoPartSequence[Long], ClientEvent]] =
          es.put(event)
      }
      snapshotStore <- getSnapshotStore
      query = clientEventStream(eventStore, snapshotStore)
      strategy = instantRetryStrategy(retryCount)
      save = DirectoryEventStream.allUsersSaveAPI(SaveAPI.Config(strategy), query)
    } yield (eventStore, save)

  val batchRetries =
    Prop.forAll(arbitrary[Client.Id], arbitrary[Client.Data], posNum[Int], posNum[Int]) { (c1: Client.Id, d1: Client.Data, batchSize: Int, retryCount: Int) =>
      val size = (batchSize % maxBatchSize) + 1
      val expected = (SaveResult.TimedOut(retryCount), Some(TwoPartSequence(size.toLong * (retryCount + 1), 0)))

      val op = Operation.insert[TwoPartSequence[Long], ClientEvent](Insert(c1, d1))

      (for {
        stores <- getStores(retryCount)
        (es, s) = stores
        eventStreamError <- s.batch(c1, NonEmptyList.nel(op, List.fill(size - 1)(op)))
        latest <- es.latest(SingleStreamKey.VAL).map(_.id.seq).run
      } yield (eventStreamError, latest)) must returnValue(expected)
    }
}
