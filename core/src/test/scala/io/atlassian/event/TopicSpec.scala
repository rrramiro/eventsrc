package io.atlassian.event

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalacheck.Prop
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.TaskMatchers
import scala.concurrent.duration._

import scalaz.concurrent.{ Task, Strategy }
import scalaz.std.list._
import scalaz.stream.Process
import scalaz.syntax.apply._
import scalaz.syntax.foldable._

import io.atlassian.event.stream.{ Event, EventId }
import io.atlassian.event.stream.memory.MemoryEventStorage

class TopicSpec extends ScalaCheckSpec {
  def is = s2"""
    Topic should
      Get concurrent events        $concurrentSubscribe
  """

  val pool = java.util.concurrent.Executors.newFixedThreadPool(5)

  val concurrentSubscribe = Prop.forAll { (xs: List[String]) =>
    (for {
      es <- MemoryEventStorage.empty[Char, Long, String]
      key = 'a'
      p = Topic[Task].atEnd(es.get(key, _: Option[Long]))
      ys <- Task.gatherUnordered(
        List(
          p.take(xs.length).runLog.map(_.toList),
          xs.zipWithIndex.traverse_ {
            case (x, i) =>
              for {
                _ <- Task.delay { Thread.sleep(2) }
                latest <- es.latest(key).map(_.id).run
                eventId = latest.fold(EventId.first(key))(EventId.next)
                now <- Task.delay { DateTime.now }
                _ <- es.put(Event(eventId, now, x))
              } yield ()
          } as List.empty
        ).map(Task.fork(_)(pool))
      )
    } yield ys.flatten) must returnValue(xs)
  }
}
