package io.atlassian.event

import scalaz.{ Enum, Monoid, Order }
import scalaz.std.anyVal._
import scalaz.syntax.monoid._
import scalaz.syntax.enum._

/**
 * TODO: Write some ScalaCheck properties.
 *
 * forall a. first <= a
 * forall a. next(a) > a
 */
trait Sequence[A] extends Order[A] {
  def first: A
  def next(a: A): A
}

object Sequence {
  def apply[S: Sequence] =
    implicitly[Sequence[S]]

  def fromMonoidEnum[S: Monoid: Enum]: Sequence[S] =
    new Sequence[S] {
      def order(x: S, y: S) = Order[S].order(x, y)
      def first = mzero[S]
      def next(s: S) = s.succ
    }

  implicit val longSequence: Sequence[Long] = fromMonoidEnum
}
