package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[S] {
  import SaveResult._

  def fold[X](s: S => X, r: NonEmptyList[Reason] => X): X =
    this match {
      case Success(v)      => s(v)
      case Reject(reasons) => r(reasons)
    }
}

object SaveResult {
  case class Success[S](s: S) extends SaveResult[S]
  case class Reject[S](reasons: NonEmptyList[Reason]) extends SaveResult[S]

  def success[S](s: S): SaveResult[S] =
    Success(s)

  def reject[S](reasons: NonEmptyList[Reason]): SaveResult[S] =
    Reject(reasons)
}
