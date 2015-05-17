package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[S, V] {
  import SaveResult._

  def fold[X](s: Snapshot[S, V] => X, r: NonEmptyList[Reason] => X, n: Snapshot[S, V] => X): X =
    this match {
      case Success(v) => s(v)
      case Reject(reasons) => r(reasons)
      case Noop(v) => n(v)
    }
}

object SaveResult {
  // TODO - separate delete and insert/update cases?
  case class Success[S, V](value: Snapshot[S, V]) extends SaveResult[S, V]
  case class Reject[S, V](reasons: NonEmptyList[Reason]) extends SaveResult[S, V]
  case class Noop[S, V](value: Snapshot[S, V]) extends SaveResult[S, V]

  def success[S, V](a: Snapshot[S, V]): SaveResult[S, V] =
    Success(a)

  def noop[S, V](a: Snapshot[S, V]): SaveResult[S, V] =
    Noop(a)

  def reject[S, V](reasons: NonEmptyList[Reason]): SaveResult[S, V] =
    Reject(reasons)
}
