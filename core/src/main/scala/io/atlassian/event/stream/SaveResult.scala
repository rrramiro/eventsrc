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
      case Success(v)      => success(v)
      case Reject(reasons) => rejected(reasons)
      case TimedOut()      => timedOut
    }
}

object SaveResult {
  case class Success[S](s: S) extends SaveResult[S]
  case class Reject[S](reasons: NonEmptyList[Reason]) extends SaveResult[S]
  case class TimedOut[S]() extends SaveResult[S]

  def success[S](s: S): SaveResult[S] =
    Success(s)

  def reject[S](reasons: NonEmptyList[Reason]): SaveResult[S] =
    Reject(reasons)

  def timedOut[S]: SaveResult[S] =
    TimedOut()
}
