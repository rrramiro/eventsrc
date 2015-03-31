package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Wraps an operation to save an event to an event stream. Saving to an event stream is through an API, which is tied
 * to an aggregate type (wrapped in a Snapshot).
 * @param run Function from a snapshot to an operation that should occur (i.e. should we save the event or reject it)
 * @tparam V The type of the aggregate.
 */
case class Operation[K, S, V, E](run: Snapshot[K, S, V] => Operation.Result[E])

object Operation {
  sealed trait Result[E] {
    import Result._

    def orElse(other: => Result[E]): Result[E] =
      fold(other, _ => other, _ => this)

    def fold[T](noop: => T, reject: NonEmptyList[Reason] => T, success: E => T): T =
      this match {
        case Success(t) => success(t)
        case Reject(r)  => reject(r)
        case Noop()     => noop
      }
  }

  object Result {
    case class Success[E](event: E) extends Result[E]
    case class Reject[E](reasons: NonEmptyList[Reason]) extends Result[E]
    case class Noop[E]() extends Result[E]

    def success[E](e: E): Result[E] =
      Success(e)

    def reject[E](r: NonEmptyList[Reason]): Result[E] =
      Reject(r)

    def noop[E]: Result[E] =
      Noop()
  }
}
