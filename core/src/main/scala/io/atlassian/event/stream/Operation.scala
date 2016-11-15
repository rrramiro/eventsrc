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
case class Operation[S, A](apply: Option[S] => Operation.Result[A])

object Operation {
  def insert[S, A](e: A): Operation[S, A] =
    Operation { _ => Result.success(e) }

  def ifSeq[S: Equal, A](seq: S, a: A): Operation[S, A] =
    Operation { oseq =>
      if (oseq === Some(seq)) Result.success(a)
      else Result.reject(Reason(s"Mismatched event stream sequence number: $oseq does not match expected $seq").wrapNel)
    }

  object syntax {
    implicit class ToOperationOps[A](val self: A) {
      def op[S]: Operation[S, A] =
        Operation.insert(self)
    }
  }

  sealed trait Result[A] {
    import Result._

    def orElse(other: => Result[A]): Result[A] =
      fold(_ => other, _ => this)

    def fold[T](reject: NonEmptyList[Reason] => T, success: A => T): T =
      this match {
        case Success(t) => success(t)
        case Reject(r)  => reject(r)
      }
  }

  object Result {
    case class Success[A](event: A) extends Result[A]
    case class Reject[A](reasons: NonEmptyList[Reason]) extends Result[A]

    def success[A](a: A): Result[A] =
      Success(a)

    def reject[A](r: NonEmptyList[Reason]): Result[A] =
      Reject(r)
  }
}
