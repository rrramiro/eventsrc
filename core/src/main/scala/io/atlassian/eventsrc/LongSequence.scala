package io.atlassian.eventsrc

import scalaz.Order
import scalaz.std.anyVal._

case class LongSequence(seq: Long)

object LongSequence {
  val first = LongSequence(0)

  def next(s: LongSequence): LongSequence =
    LongSequence(s.seq + 1)

  implicit val LongSequenceSequence: Sequence[LongSequence] =
    new Sequence[LongSequence] {
      val first = LongSequence.first

      def next(s: LongSequence): LongSequence =
        LongSequence.next(s)

      val order: Order[LongSequence] =
        Order.orderBy { _.seq }
    }
}
