package io.atlassian.event.stream

sealed trait QueryConsistency {
  import QueryConsistency._

  def fold[X](snapshot: => X, event: => X): X =
    this match {
      case LatestSnapshot => snapshot
      case LatestEvent    => event
    }
}
object QueryConsistency {
  case object LatestSnapshot extends QueryConsistency
  case object LatestEvent extends QueryConsistency

  val latestSnapshot: QueryConsistency = LatestSnapshot
  val latestEvent: QueryConsistency = LatestEvent
}
