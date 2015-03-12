package io.atlassian.eventsrc

import scalaz.{ @@, Tag }

trait EventTypes {
  sealed trait ReasonTag
  type Reason = String @@ ReasonTag
  object Reason {
    def apply(m: String): Reason = Tag.apply(m)
  }
}
