package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * Result of saving an event to the stream.
 * @tparam A The aggregate type.
 */
sealed trait SaveResult[K, S, V]

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
