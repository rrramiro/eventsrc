package io.atlassian.event
package stream
package dynamo

import scalaz.std.option._
import scalaz.syntax.std.option._

/**
 * ADT for type of snapshot so we can save it as a separate column.
 */
private[dynamo] sealed trait SnapshotType

private[dynamo] object SnapshotType {
  case object NoSnapshot extends SnapshotType
  case object Value extends SnapshotType
  case object Deleted extends SnapshotType

  def apply(t: SnapshotType): String =
    t match {
      case NoSnapshot => "none"
      case Value      => "value"
      case Deleted    => "deleted"
    }

  def unapply(s: String): Option[SnapshotType] =
    s.toLowerCase match {
      case "none"    => NoSnapshot.some
      case "value"   => Value.some
      case "deleted" => Deleted.some
      case _         => none
    }
}