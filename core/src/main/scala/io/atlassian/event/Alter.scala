package io.atlassian.event

import scalaz.syntax.invariantFunctor._
import scalaz.{ Plus, Semigroup }

case class Alter[F[_], A](run: F[A])

object Alter {
  implicit def AlterSemigroup[F[_]: Plus, A]: Semigroup[Alter[F, A]] =
    Plus[F].semigroup[A].xmap(Alter.apply, _.run)
}
