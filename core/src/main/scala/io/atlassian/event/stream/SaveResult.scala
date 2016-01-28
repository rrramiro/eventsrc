package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[S] {
  import SaveResult._

  def fold[X](success: (S, Int) => X, rejected: (NonEmptyList[Reason], Int) => X, timedOut: Int => X): X =
    this match {
      case Success(v, c)      => success(v, c)
      case Reject(reasons, c) => rejected(reasons, c)
      case TimedOut(c)        => timedOut(c)
    }
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
