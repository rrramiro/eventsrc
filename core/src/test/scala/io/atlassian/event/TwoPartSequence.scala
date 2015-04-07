package io.atlassian.event

import scalaz.Order
import scalaz.Ordering
import scalaz.std.anyVal._

/**
 * An example of a sequence that is not a simple number. In this case it is a composite between a sequence number
 * and a 'zone' number that is constant for a given client
 */
case class TwoPartSequence(seq: Long, zone: Long)

object TwoPartSequence {

  def twoPartSequence(zone: Long): Sequence[TwoPartSequence] =
    new Sequence[TwoPartSequence] {
      override def next(a: TwoPartSequence): TwoPartSequence =
        a.copy(seq = a.seq + 1)

      override val order: Order[TwoPartSequence] =
        Order.order { (a, b) =>
          Order[Long].apply(a.seq, b.seq) match {
            case Ordering.EQ => Order[Long].apply(a.zone, b.zone)
            case o => o
          }
        }

      override val first: TwoPartSequence = TwoPartSequence(0, zone)
    }
}
