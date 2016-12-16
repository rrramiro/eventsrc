package io.atlassian.event
package stream

import scalaz.{ Applicative, Bind, Monoid, NonEmptyList, Traverse, Show }
import scalaz.syntax.all._

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[A] {
  import SaveResult._

  def fold[X](success: A => X, rejected: NonEmptyList[Reason] => X, timedOut: => X): X =
    this match {
      case Success(v, _)      => success(v)
      case Reject(reasons, _) => rejected(reasons)
      case TimedOut(_)        => timedOut
    }

  def retryCount: Int

  def canRetry: Boolean =
    fold(_ => false, _ => false, true)
}

object SaveResult {
  case class Success[A](a: A, retryCount: Int) extends SaveResult[A]
  case class Reject[A](reasons: NonEmptyList[Reason], retryCount: Int) extends SaveResult[A]
  case class TimedOut[A](retryCount: Int) extends SaveResult[A]

  def success[A](a: A, retryCount: Int): SaveResult[A] =
    Success(a, retryCount)

  def reject[A](reasons: NonEmptyList[Reason], retryCount: Int): SaveResult[A] =
    Reject(reasons, retryCount)

  def timedOut[A](retryCount: Int): SaveResult[A] =
    TimedOut(retryCount)

  def fromOperationResult[A](retryCount: Int)(r: Operation.Result[A]): SaveResult[A] =
    r.fold(SaveResult.Reject(_, retryCount), SaveResult.Success(_, retryCount))

  def fromEventStreamError[A](retryCount: Int)(e: EventStreamError): SaveResult[A] =
    e match {
      case EventStreamError.DuplicateEvent => SaveResult.timedOut[A](retryCount)
      case EventStreamError.Rejected(r)    => SaveResult.reject[A](r, retryCount)
    }

  implicit def ShowSaveResult[A: Show]: Show[SaveResult[A]] =
    Show.showFromToString

  implicit def MonoidSaveResult[A: Sequence: Show]: Monoid[SaveResult[A]] =
    new Monoid[SaveResult[A]] {
      private def reason(result: SaveResult[A]) =
        Reason(Show[SaveResult[A]].shows(result))

      override def zero: SaveResult[A] = Success(Sequence[A].first, 0)

      override def append(f1: SaveResult[A], f2: => SaveResult[A]): SaveResult[A] =
        (f1, f2) match {
          case (Success(a, i), Success(b, j)) => Success(Sequence[A].max(a, b), i + j)
          case (Success(_, _), r)             => r
          case (r, Success(_, _))             => r
          case (Reject(a, i), Reject(b, j))   => Reject(a |+| b, i + j)
          case (Reject(as, j), TimedOut(i))   => Reject(reason(timedOut(i)) <:: as, i + j)
          case (TimedOut(i), TimedOut(j))     => Reject(NonEmptyList(reason(timedOut(i)), reason(timedOut(j))), i + j)
          case (TimedOut(i), Reject(as, j))   => Reject(reason(timedOut(i)) <:: as, i + j)
        }
    }

  implicit val SaveResultMonad: Bind[SaveResult] with Traverse[SaveResult] = new Bind[SaveResult] with Traverse[SaveResult] {
    override def map[A, B](fa: SaveResult[A])(f: A => B): SaveResult[B] =
      fa match {
        case Success(a, retryCount)      => Success(f(a), retryCount)
        case Reject(reasons, retryCount) => Reject(reasons, retryCount)
        case TimedOut(retryCount)        => TimedOut(retryCount)
      }

    def traverseImpl[G[_]: Applicative, A, B](fa: SaveResult[A])(f: A => G[B]): G[SaveResult[B]] =
      fa match {
        case Success(a, retryCount)      => f(a).map(Success(_, retryCount))
        case Reject(reasons, retryCount) => reject(reasons, retryCount).point[G]
        case TimedOut(retryCount)        => timedOut(retryCount).point[G]
      }

    def bind[A, B](fa: SaveResult[A])(f: A => SaveResult[B]): SaveResult[B] =
      fa match {
        case Success(a, retryCount)      => f(a)
        case Reject(reasons, retryCount) => Reject(reasons, retryCount)
        case TimedOut(retryCount)        => TimedOut(retryCount)
      }
  }
}
