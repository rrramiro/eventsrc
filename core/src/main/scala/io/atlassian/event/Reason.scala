package io.atlassian.event

import scalaz.{ Equal, Show }
import scalaz.std.string._
import scalaz.syntax.show._

case class Reason private (s: String) extends AnyVal

object Reason {
  def apply[A: Show](a: A) =
    new Reason(a.shows)

  implicit val reasonEqual: Equal[Reason] =
    Equal[String].contramap(_.s)
}
