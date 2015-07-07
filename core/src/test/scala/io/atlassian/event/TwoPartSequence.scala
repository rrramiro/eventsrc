package io.atlassian.event

import scalaz.std.anyVal._
import scalaz.syntax.monoid._
import scalaz.syntax.order._
import scalaz.{ Order, Ordering, Monoid }

/**
 * An example of a sequence that is not a simple number. In this case it is a composite between a sequence number
 * and a 'zone' that is constant for a given client
 */
case class TwoPartSequence[Z](seq: Long, zone: Z)

object TwoPartSequence {
  implicit def TwoPartSequenceSequence[Z: Sequence]: Sequence[TwoPartSequence[Z]] =
    new Sequence[TwoPartSequence[Z]] {
      def order(x: TwoPartSequence[Z], y: TwoPartSequence[Z]) =
        x.seq cmp y.seq mappend (x.zone cmp y.zone)

      val first: TwoPartSequence[Z] = TwoPartSequence(0, Sequence[Z].first)

      def next(a: TwoPartSequence[Z]): TwoPartSequence[Z] =
        a.copy(seq = a.seq + 1)
    }
}
