package io.atlassian.event

import scalaz.Order
import scalaz.std.anyVal._

trait Sequence[A] {
  def first: A
  def next(a: A): A
  def order: Order[A]
}

object Sequence {
  def apply[S: Sequence] =
    implicitly[Sequence[S]]

  implicit object LongSequence extends Sequence[Long] {
    val first = 0L
    def next(s: Long): Long = s + 1
    val order: Order[Long] = Order[Long]
  }
}

sealed trait SequenceQuery[S]
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