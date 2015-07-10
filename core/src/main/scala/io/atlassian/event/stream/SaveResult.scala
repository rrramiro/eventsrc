package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[S] {
  import SaveResult._

  def fold[X](s: S => X, r: NonEmptyList[Reason] => X, n: Option[S] => X): X =
    this match {
      case Success(v) => s(v)
      case Reject(reasons) => r(reasons)
      case Noop(v) => n(v)
    }
}

object SaveResult {
  // TODO - separate delete and insert/update cases?
  case class Success[S](s: S) extends SaveResult[S]
  case class Reject[S](reasons: NonEmptyList[Reason]) extends SaveResult[S]
  case class Noop[S](s: Option[S]) extends SaveResult[S]

  def success[S](s: S): SaveResult[S] =
    Success(s)

  def noop[S](s: Option[S]): SaveResult[S] =
    Noop(s)

  def reject[S](reasons: NonEmptyList[Reason]): SaveResult[S] =
    Reject(reasons)
}
