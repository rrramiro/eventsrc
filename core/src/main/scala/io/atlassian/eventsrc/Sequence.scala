package io.atlassian.eventsrc

import scalaz.Order

trait Sequence[A] {
  def first: A
  def next(a: A): A

  def order: Order[A]
}
