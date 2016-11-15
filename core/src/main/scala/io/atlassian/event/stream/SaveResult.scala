package io.atlassian.event
package stream

import scalaz.NonEmptyList

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
}
