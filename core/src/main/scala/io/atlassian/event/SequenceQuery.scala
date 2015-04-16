package io.atlassian.event

/**
 * Wraps how we can query snapshots by sequence number:
 *   - before S - we want snapshot before sequence number S (exclusive)
 *   - earliest - we want to earliest snapshot stored i.e. this would be the current epoch.
 *   - latest - we want the latest snapshot stored. This is the most common case.
 */
sealed trait SequenceQuery[S] {
  import SequenceQuery._

  def fold[X](before: S => X, earliest: => X, latest: => X): X =
    this match {
      case Before(s) => before(s)
      case Earliest() => earliest
      case Latest() => latest
    }
}

object SequenceQuery {
  case class Before[S](s: S) extends SequenceQuery[S]
  case class Earliest[S]() extends SequenceQuery[S]
  case class Latest[S]() extends SequenceQuery[S]

  def before[S](s: S): SequenceQuery[S] =
    Before(s)

  def earliest[S]: SequenceQuery[S] =
    Earliest()

  def latest[S]: SequenceQuery[S] =
    Latest()
}