package io.atlassian.event
package stream

import scalaz.{ Catchable, Equal, MonadPlus, Monoid, \/ }

import scalaz.syntax.either._
import scalaz.syntax.monadPlus._
import scalaz.syntax.monoid._
import scalaz.syntax.order._

sealed trait SafeCatchable[A] {
  def toOption: Option[A]
  def get(implicit M: Monoid[A]): A
}

object SafeCatchable {
  def success[A](a: A): SafeCatchable[A] =
    Success(a)

  def failure[A]: SafeCatchable[A] =
    Failure()

  case class Success[A](value: A) extends SafeCatchable[A] {
    def toOption: Option[A] =
      Some(value)

    def get(implicit M: Monoid[A]): A =
      value
  }

  case class Failure[A]() extends SafeCatchable[A] {
    def toOption: Option[A] =
      None

    def get(implicit M: Monoid[A]): A =
      mzero[A]
  }

  implicit val safeCatchableMonadPlus: MonadPlus[SafeCatchable] =
    new MonadPlus[SafeCatchable] {
      def point[A](a: => A): SafeCatchable[A] =
        Success(a)

      def bind[A, B](fa: SafeCatchable[A])(f: A => SafeCatchable[B]): SafeCatchable[B] =
        fa match {
          case Success(value) => f(value)
          case Failure()      => Failure()
        }

      def empty[A]: SafeCatchable[A] =
        Failure[A]()

      def plus[A](a: SafeCatchable[A], b: => SafeCatchable[A]): SafeCatchable[A] =
        a match {
          case Success(value) => Success(value)
          case Failure()      => b
        }
    }

  implicit val safeCatchableCatchable: Catchable[SafeCatchable] =
    new Catchable[SafeCatchable] {
      def attempt[A](f: SafeCatchable[A]): SafeCatchable[Throwable \/ A] =
        f.map(_.right)

      def fail[A](err: Throwable): SafeCatchable[A] =
        mempty[SafeCatchable, A]
    }

  implicit def safeCatchableEqual[A: Equal]: Equal[SafeCatchable[A]] =
    new Equal[SafeCatchable[A]] {
      def equal(a1: SafeCatchable[A], a2: SafeCatchable[A]) =
        (a1, a2) match {
          case (Success(v1), Success(v2)) => v1 === v2
          case (Failure(), Failure())     => true
          case (_, _)                     => false
        }
    }
}
