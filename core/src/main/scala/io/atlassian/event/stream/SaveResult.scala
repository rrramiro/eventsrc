package io.atlassian.event
package stream

import scalaz.{ Monoid, NonEmptyList, Show }
import scalaz.syntax.monoid._
import scalaz.syntax.show._

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

  implicit def ShowSaveResult[A: Show]: Show[SaveResult[A]] =
    Show.showFromToString

  implicit def MonoidSaveResult[A: Sequence]: Monoid[SaveResult[A]] =
    new Monoid[SaveResult[A]] {
      override def zero: SaveResult[A] = Success(Sequence[A].first, 0)

      override def append(f1: SaveResult[A], f2: => SaveResult[A]): SaveResult[A] =
        (f1, f2) match {
          case (Success(a, i), Success(b, j)) => Success(Sequence[A].max(a, b), i + j)
          case (Success(_, _), r)             => r
          case (r, Success(_, _))             => r
          case (Reject(a, i), Reject(b, j))   => Reject(a |+| b, i + j)
          case (Reject(as, j), TimedOut(i))   => Reject(Reason(timedOut(i).toString()) <:: as, i + j)
          case (TimedOut(i), TimedOut(j))     => Reject(NonEmptyList(Reason(timedOut(i).toString()), Reason(timedOut(j).toString())), i + j)
          case (TimedOut(i), Reject(as, j))   => Reject(Reason(timedOut(i).toString()) <:: as, i + j)
        }
    }
}
