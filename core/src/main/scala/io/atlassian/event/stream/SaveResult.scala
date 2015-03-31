package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 */
sealed trait SaveResult[K, S, V] {
  import SaveResult._

  def fold[X](s: Snapshot[K, S, V] => X, r: NonEmptyList[Reason] => X, n: => X): X =
    this match {
      case Success(v) => s(v)
      case Reject(reasons) => r(reasons)
      case Noop() => n
    }
}

object SaveResult {
  // TODO - separate delete and insert/update cases?
  case class Success[K, S, V](value: Snapshot[K, S, V]) extends SaveResult[K, S, V]
  case class Reject[K, S, V](reasons: NonEmptyList[Reason]) extends SaveResult[K, S, V]
  case class Noop[K, S, V]() extends SaveResult[K, S, V]

  def success[K, S, V](a: Snapshot[K, S, V]): SaveResult[K, S, V] =
    Success(a)

  def noop[K, S, V](): SaveResult[K, S, V] =
    Noop()

  def reject[K, S, V](reasons: NonEmptyList[Reason]): SaveResult[K, S, V] =
    Reject(reasons)
}
