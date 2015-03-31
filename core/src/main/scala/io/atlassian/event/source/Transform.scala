package io.atlassian.event
package source

import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.{ Equal, Functor, Monad }

/**
 * Transform essentially wraps a function from Option[A] => Option[A], but adds a little structure so that it can be
 * stored easily in a commit object.
 * @tparam A
 */
sealed trait Transform[+A] {
  import Transform._
  def value: Option[A] =
    this match {
      case Insert(a) => a.some
      case Delete    => none
    }

  def map[B](f: A => B) =
    this match {
      case Insert(a) => Insert(f(a))
      case Delete    => Delete
    }
}

object Transform {
  def delete[A]: Transform[A] = Delete
  case class Insert[A](a: A) extends Transform[A]
  case object Delete extends Transform[Nothing]

  implicit object TransformFunctor extends Functor[Transform] {
    def map[A, B](fa: Transform[A])(f: A => B) = fa map f
  }

  sealed trait Op
  object Op {
    case object Insert extends Op
    case object Delete extends Op

    def unapply(s: String): Option[Op] =
      s.toLowerCase match {
        case "insert" => Some(Insert)
        case "delete" => Some(Delete)
        case _        => None
      }

    def apply(op: Op): String =
      op match {
        case Insert => "insert"
        case Delete => "delete"
      }

    implicit val EqualsOp = Equal.equalA[Op]
  }
}
