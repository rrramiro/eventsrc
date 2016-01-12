package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[S] {
  import SaveResult._

  def fold[X](s: S => X, r: NonEmptyList[Reason] => X, t: => X): X =
    this match {
      case Success(v)      => s(v)
      case Reject(reasons) => r(reasons)
      case TimedOut()      => t
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
