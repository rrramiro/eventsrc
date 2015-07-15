package io.atlassian.event

import org.joda.time.DateTime
import scala.concurrent.duration.FiniteDuration

import scalaz.concurrent.Task
import scalaz.stream.async.mutable.{ Topic => ScalazTopic }
import scalaz.stream.time
import scalaz.stream.{ Cause, DefaultScheduler, Process, Sink }
import scalaz.syntax.applicative._

import io.atlassian.event.stream.{ Event, EventId, EventStorage }

object Topic {
  /**
   * Concatenates processes together, using the last sequence ID
   * to generate a new tail.
   */
  // WartRemover bug causes problems for @-patterns.
  @SuppressWarnings(Array(
    "org.brianmckenna.wartremover.warts.IsInstanceOf",
    "org.brianmckenna.wartremover.warts.AsInstanceOf"
  ))
  def atEnd[KK, S, E](f: Option[S] => Process[Task, Event[KK, S, E]]): Process[Task, E] = {
    def go(p: Process[Task, Event[KK, S, E]], s: Option[S]): Process[Task, E] =
      p.step match {
        case Process.Step(awt @ Process.Await(_, _), cont) =>
          awt.extend(p => go(p +: cont, s))
        case Process.Step(emt @ Process.Emit(e), cont) =>
          emt.map(_.operation) ++
            go(cont.continue, e.lastOption.map(_.id.seq))
        case hlt @ Process.Halt(Cause.End) =>
          go(f(s), s)
        case hlt @ Process.Halt(_) =>
          hlt
      }

    go(f(None), None)
  }
}
