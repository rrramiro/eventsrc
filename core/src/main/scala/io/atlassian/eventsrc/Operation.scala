package io.atlassian.eventsrc

import scalaz.{ Functor, \/, Equal, NonEmptyList }
import scalaz.syntax.equal._

/**
 * Data-type that represents an operation that can be saved to an `EventSource.API`.
 *
 * This type represents conditionally applied transformations of the data, so
 * it is possible to have check-then-act semantics for all updates, including
 * insert only if absent, and replace if value matches.
 *
 * There is syntax for creating Operations, eg. `"foo".insertOp` and `deleteOp` that
 * is accessible via `import Operation.syntax._`
 */
case class Operation[A](run: Option[A] => Operation.Result[A]) {
  def apply(opt: Option[A]): Operation.Result[A] =
    run(opt)

  def filter(p: A => Boolean, becauseReasons: => NonEmptyList[Reason]) =
    Operation[A] { opt =>
      if (opt.isDefined)
        if (opt.exists(p)) run(opt)
        else Operation.Result.Reject(becauseReasons)
      else Operation.Result.Noop()
    }

  def mapEqual[B: Equal](f: A => B)(b: B) =
    filter(f(_) === b, NonEmptyList(Reason(s"value doesn't equal '$b'")))

  def ifEqual(a: A)(implicit eq: Equal[A]) =
    mapEqual(identity)(a)

  def ifExists =
    filter(_ => true, NonEmptyList(Reason("value doesn't exist")))

  def ifAbsent =
    Operation[A] { opt => if (opt.isEmpty) run(opt) else Operation.Result.Noop() }

  def orElse(or: Operation[A]) =
    Operation[A] { opt => run(opt) orElse or(opt) }

  /** alias for `orElse` */
  def ||(or: Operation[A]) =
    orElse(or)

  /** InvariantFunctor scalaz 7.1.0 */
  def xmap[B](f: A => B, g: B => A) =
    Operation[B] { opt => run { opt map g } map f }

  def condition(p: Option[A] => Boolean)(reasons: => NonEmptyList[Reason]) =
    Operation[A] { opt =>
      if (p(opt)) run(opt) else Operation.Result.Reject(reasons)
    }

  def condition[B](p: Option[A] => NonEmptyList[Reason] \/ B) =
    Operation[A] { opt =>
      p(opt).fold(Operation.Result.Reject.apply, _ => run(opt))
    }

}

object Operation {

  def insert[A](a: A) =
    Operation[A] { _ => Transform.Insert(a).success }

  def delete[A] =
    Operation[A] { _ => Transform.delete[A].success }.ifExists

  object syntax {
    implicit class ToOperationOps[A](val self: A) extends AnyVal {
      def insertOp: Operation[A] =
        Operation.insert(self)

      def updateOp(onlyIf: A => Boolean, reason: Reason = Reason("failed update predicate")): Operation[A] =
        Operation.insert(self).filter(onlyIf, NonEmptyList(reason))
    }

    def deleteOp[A]: Operation[A] =
      Operation.delete
  }

  sealed trait Result[A] {
    import Result._
    def map[B](f: A => B): Result[B] =
      this match {
        case Success(t) => Success(t map f)
        case Reject(r)  => Reject(r)
        case Noop()     => Noop()
      }

    def orElse(other: => Result[A]): Result[A] =
      this match {
        case s @ Success(_) => s
        case _              => other
      }

    def fold[T](noop: => T, reject: NonEmptyList[Reason] => T, success: Transform[A] => T): T =
      this match {
        case Success(t) => success(t)
        case Reject(r)  => reject(r)
        case Noop()     => noop
      }
  }

  object Result {
    case class Success[A](transform: Transform[A]) extends Result[A]
    case class Reject[A](reasons: NonEmptyList[Reason]) extends Result[A]
    case class Noop[A]() extends Result[A]

    implicit object ResultFunctor extends Functor[Result] {
      def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa map f
    }
  }

  implicit class TransformSyntax[A](val t: Transform[A]) extends AnyVal {
    def success[B >: A]: Result[B] =
      Result.Success(t)
  }
}