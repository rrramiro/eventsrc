package io.atlassian.event

import scalaz.{ Applicative, Bifunctor, Plus, Semigroup }
import scalaz.syntax.applicative._
import scalaz.syntax.bifunctor._

case class Alter[F[_], A](run: F[A])

object Alter {
  implicit def AlterSemigroup[F[_]: Plus, A]: Semigroup[Alter[F, A]] =
    Plus[F].semigroup[A].xmap(Alter.apply, _.run)

  implicit def AlterApplicative[F[_]: Applicative]: Applicative[Alter[F, ?]] =
    new Applicative[Alter[F, ?]] {
      def point[A](a: => A): Alter[F, A] =
        Alter(a.point[F])

      def ap[A, B](fa: => Alter[F, A])(f: => Alter[F, A => B]): Alter[F, B] =
        Alter(fa.run <*> f.run)
    }

  def bialter[M[_, _]: Bifunctor, F[_], A, B](fab: M[F[A], F[B]]): M[Alter[F, A], Alter[F, B]] =
    fab.bimap(Alter.apply, Alter.apply)
}
