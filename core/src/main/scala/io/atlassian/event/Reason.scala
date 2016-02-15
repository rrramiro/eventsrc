package io.atlassian.event

import scalaz.{ @@, Equal, Tag }
import scalaz.std.string._
import scalaz.syntax.equal._

case class Reason(s: String) extends AnyVal

object Reason {
  implicit val reasonEqual: Equal[Reason] =
    new Equal[Reason] {
      def equal(a1: Reason, a2: Reason): Boolean =
        a1.s === a2.s
    }
}
