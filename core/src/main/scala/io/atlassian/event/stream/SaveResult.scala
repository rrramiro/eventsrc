package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[S] {
  import SaveResult._

  def fold[X](success: S => X, rejected: NonEmptyList[Reason] => X, timedOut: => X): X =
    this match {
      case Success(v, _)      => success(v)
      case Reject(reasons, _) => rejected(reasons)
      case TimedOut(_)        => timedOut
    }

  def retryCount: Int
}

object SaveResult {
  case class Success[S](s: S, retryCount: Int) extends SaveResult[S]
  case class Reject[S](reasons: NonEmptyList[Reason], retryCount: Int) extends SaveResult[S]
  case class TimedOut[S](retryCount: Int) extends SaveResult[S]

  def success[S](s: S, retryCount: Int): SaveResult[S] =
    Success(s, retryCount)

  def reject[S](reasons: NonEmptyList[Reason], retryCount: Int): SaveResult[S] =
    Reject(reasons, retryCount)

  def timedOut[S](retryCount: Int): SaveResult[S] =
    TimedOut(retryCount)
}
