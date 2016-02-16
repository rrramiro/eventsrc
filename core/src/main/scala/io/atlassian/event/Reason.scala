package io.atlassian.event

import scalaz.{ @@, Equal, Tag }
import scalaz.std.string._
import scalaz.syntax.equal._

case class Reason(s: String) extends AnyVal

object Reason {
  implicit val reasonEqual: Equal[Reason] =
    Equal[String].contramap(_.s)
}
