package io.atlassian.event
package stream

import scalaz.NonEmptyList
import scalaz.syntax.nel._

/**
 * Wraps an operation to save an event to an event stream. Saving to an event stream is through an API, which is tied
 * to an aggregate type (wrapped in a Snapshot).
 * @param run Function from a snapshot to an operation that should occur (i.e. should we save the event or reject it)
 * @tparam V The type of the aggregate.
 */
case class Operation[K, S, V, E](run: Snapshot[K, S, V] => Operation.Result[E]) {
  def apply(s: Snapshot[K, S, V]): Operation.Result[E] =
    run(s)

  def filter(p: V => Boolean, becauseReasons: => NonEmptyList[Reason]): Operation[K, S, V, E] =
    Operation { s =>
      if (s.value.isDefined)
        if (s.value.exists(p)) run(s)
        else Operation.Result.Reject(becauseReasons)
      else Operation.Result.Noop()
    }

  def filter(v: DataValidator.Validator[V]): Operation[K, S, V, E] =
    Operation { s =>
      v(s.value).fold(
        reasons => Operation.Result.reject(reasons),
        _ => run(s)
      )
    }
}

object Operation {
  def insert[K, S, V, E](e: E): Operation[K, S, V, E] =
    Operation { _ => Result.success(e) }

  def ifSeq[K, S, V, E](seq: S, e: E): Operation[K, S, V, E] =
    Operation { snapshot =>
      if (snapshot.seq == seq) Result.success(e)
      else Result.reject(Reason(s"Mismatched event stream sequence number: ${snapshot.seq} does not match expected $seq").wrapNel)
    }

  object syntax {
    implicit class ToOperationOps[E](val self: E) {
      def op[K, S, V]: Operation[K, S, V, E] =
        Operation.insert(self)

      def updateOp[K, S, V](onlyIf: V => Boolean, reason: Reason = Reason("failed update predicate")): Operation[K, S, V, E] =
        Operation.insert(self).filter(onlyIf, NonEmptyList(reason))

      def updateOp[K, S, V](validator: DataValidator.Validator[V]): Operation[K, S, V, E] =
        Operation.insert(self).filter(validator)
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
