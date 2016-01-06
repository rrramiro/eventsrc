package io.atlassian.event
package stream

import scalaz.NonEmptyList

/**
 * EventStreamError represents any error conditions that are useful to represent for event sources. In particular,
 * we need to know about attempts to store duplicate events.
 */
sealed trait EventStreamError
object EventStreamError {
  def reject(s: NonEmptyList[Reason]): EventStreamError = Rejected(s)

  val duplicate: EventStreamError = DuplicateEvent

  case object DuplicateEvent extends EventStreamError

  case class Rejected(s: NonEmptyList[Reason]) extends EventStreamError
}
