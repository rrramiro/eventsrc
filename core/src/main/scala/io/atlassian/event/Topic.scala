package io.atlassian.event

import scalaz.stream.{ Cause, Process }

import io.atlassian.event.stream.Event

object Topic {
  def apply[F[_]] =
    new Topic[F] {}
}

trait Topic[F[_]] {
  /**
   * Concatenates processes together, using the last sequence ID
   * to generate a new tail.
   */
  // WartRemover bug causes problems for @-patterns.
  @SuppressWarnings(Array(
    "org.brianmckenna.wartremover.warts.IsInstanceOf",
    "org.brianmckenna.wartremover.warts.AsInstanceOf"
  ))
  def atEnd[KK, S, E](next: Option[S] => Process[F, Event[KK, S, E]]): Process[F, E] = {
    def go(proc: Process[F, Event[KK, S, E]], s: Option[S]): Process[F, E] =
      proc.step match {
        case Process.Step(await @ Process.Await(_, _, _), cont) => await.extend { p => go(p +: cont, s) }
        case Process.Step(emit @ Process.Emit(e), c)            => emit.map { _.operation } ++ go(c.continue, e.lastOption.map { _.id.seq })
        case Process.Halt(Cause.End)                            => go(next(s), s)
        case halt @ Process.Halt(_)                             => halt
      }

    go(next(None), None)
  }
}
