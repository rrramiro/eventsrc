package io.atlassian.event
package stream

import scalaz.{ Equal, NonEmptyList }
import scalaz.syntax.all._
import scalaz.std.option._

/**
 * Wraps an operation to save an event to an event stream. Saving to an event stream is through an API, which is tied
 * to an aggregate type.
 * @param apply Function from a sequence to an operation that should occur (i.e. should we save the event or reject it)
 */
case class Operation[S, E](apply: Option[S] => Operation.Result[E])

object Operation {
  def insert[S, E](e: E): Operation[S, E] =
    Operation { _ => Result.success(e) }

  def ifSeq[S: Equal, E](seq: S, e: E): Operation[S, E] =
    Operation { oseq =>
      if (oseq === Some(seq)) Result.success(e)
      else Result.reject(Reason(s"Mismatched event stream sequence number: $oseq does not match expected $seq").wrapNel)
    }

  object syntax {
    implicit class ToOperationOps[E](val self: E) {
      def op[S]: Operation[S, E] =
        Operation.insert(self)
    }
  }

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
