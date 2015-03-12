package io.atlassian.eventsrc

import scalaz.Order
import scalaz.std.anyVal._

trait Sequence[A] {
  def first: A
  def next(a: A): A
  def order: Order[A]
}

object Sequence {
  implicit object LongSequence extends Sequence[Long] {
    val first = 0L
    def next(s: Long): Long = s + 1
    val order: Order[Long] = Order[Long]
  }
}