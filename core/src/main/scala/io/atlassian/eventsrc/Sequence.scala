package io.atlassian.eventsrc

import scalaz.Equal
import scalaz.std.anyVal._

case class Sequence(seq: Long)

object Sequence {
  val first = Sequence(0)

  def next(s: Sequence): Sequence =
    Sequence(s.seq + 1)

  implicit val SequenceEquals: Equal[Sequence] =
    Equal.equalBy { _.seq }
}