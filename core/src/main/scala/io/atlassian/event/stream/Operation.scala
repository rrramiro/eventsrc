package io.atlassian.event
package stream

import scalaz.{ \/, EitherT, Equal, Monad, NonEmptyList, Traverse }
import scalaz.effect.IO
import scalaz.syntax.all._
import scalaz.std.option._

/**
 * Wraps an operation to save an event to an event stream. Saving to an event stream is through an API, which is tied
 * to an aggregate type.
 * @param apply Function from a sequence to an operation that should occur (i.e. should we save the event or reject it)
 */
case class Operation[S, A](run: S => Operation.Result[A]) {
  def apply(op: S): Operation.Result[A] = run(op)
}

object Operation {
  def insert[S, A](e: A): Operation[S, A] =
    Operation { _ => Result.success(e) }

  def ifSeq[S: Equal, A](seq: S, a: A): Operation[S, A] =
    Operation { oseq =>
      if (oseq === seq) Result.success(a)
      else Result.reject(Reason(s"Mismatched event stream sequence number: $oseq does not match expected $seq").wrapNel)
    }

  def enumerate[KK, S: Sequence, E](key: KK, latest: S, ops: NonEmptyList[Operation[S, E]]): IO[Operation.Result[NonEmptyList[Event[KK, S, E]]]] = {
    type Error = NonEmptyList[Reason]
    type Ev = Event[KK, S, E]
    type Evs = NonEmptyList[Ev]
    type EIO[A] = EitherT[IO, Error, A]

    def event(s: S, e: E): IO[Ev] =
      Event.next(key, Some(s), e)

    def transform(s: S)(op: Operation[S, E]): EIO[Ev] =
      EitherT(op(s).toDisjunction.traverse(e => event(s, e)))

    foldMapLeft1M[NonEmptyList, EIO, Operation[S, E], Evs](ops)(transform(latest)(_).map(NonEmptyList(_))) { (nel, op) =>
      val s = nel.head.id.seq
      transform(s)(op).map(_ <:: nel)
    }.map(_.reverse).run.map(Result.fromDisjunction)
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

    def toDisjunction: NonEmptyList[Reason] \/ A =
      fold(_.left, _.right)
  }

  object Result {
    case class Success[A](event: A) extends Result[A]
    case class Reject[A](reasons: NonEmptyList[Reason]) extends Result[A]

    def fromDisjunction[A](d: NonEmptyList[Reason] \/ A): Result[A] =
      d.fold(Reject.apply, Success.apply)

    def success[A](a: A): Result[A] =
      Success(a)

    def reject[A](r: NonEmptyList[Reason]): Result[A] =
      Reject(r)

    implicit val ResultInstances: Monad[Result] with Traverse[Result] = new Monad[Result] with Traverse[Result] {
      def point[A](a: => A): Result[A] =
        success(a)

      def bind[A, B](fa: Result[A])(f: A => Result[B]): Result[B] =
        fa match {
          case Success(event) =>
            f(event)
          case Reject(reasons) =>
            Reject(reasons)
        }

      def traverseImpl[G[_], A, B](fa: Result[A])(f: A => G[B])(implicit G: scalaz.Applicative[G]): G[Result[B]] =
        fa match {
          case Success(event) =>
            f(event).map(Success.apply)
          case Reject(reasons) =>
            G.point(Reject(reasons))
        }
    }
  }
}
