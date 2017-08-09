package io.atlassian.event
package stream

import scalaz.{ Equal, NonEmptyList }
import scalaz.syntax.equal._

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

  implicit val eventStreamErrorEqual: Equal[EventStreamError] =
    new Equal[EventStreamError] {
      def equal(a1: EventStreamError, a2: EventStreamError): Boolean =
        (a1, a2) match {
          case (DuplicateEvent, DuplicateEvent) => true
          case (Rejected(s1), Rejected(s2))     => s1 === s2
          case (_, _)                           => false
        }
    }
}
