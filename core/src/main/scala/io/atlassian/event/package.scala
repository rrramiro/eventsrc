package io.atlassian

import scalaz._
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._
import scalaz.syntax.either._
import scalaz.syntax.foldable1._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

package object event {
  implicit class TaggedOps[A, T](val a: A @@ T) extends AnyVal {
    def unwrap: A = Tag.unwrap(a)
  }

  def foldMapLeft1M[F[_], G[_], A, B](fa: F[A])(z: A => G[B])(f: (B, A) => G[B])(implicit F: Foldable1[F], G: Bind[G]): G[B] =
    F.foldMapLeft1(fa)(z) { (b, a) =>
      G.bind(b)(f(_, a))
    }

  def bifpoint[F[_, _]: Bifunctor, G[_]: Applicative, A, B](fa: F[A, B]): F[G[A], G[B]] =
    fa.bimap(_.point[G], _.point[G])

  def biIdT[M[_, _]: Bifunctor, F[_], A, B](fab: M[F[A], F[B]]): M[IdT[F, A], IdT[F, B]] =
    fab.bimap(IdT.apply, IdT.apply)

  def eitherTSeparate1[F[_]: Foldable1: Applicative: Plus, G[_]: Bind, A, B](fa: F[EitherT[G, A, B]]): EitherT[G, F[A], F[B]] = {
    // note, if this fails scalaz has probably implemented this instance on IdT
    implicit def IdTSemigroup[F[_]: Plus, A]: Semigroup[IdT[F, A]] =
      Plus[F].semigroup[A].xmap(IdT.apply, _.run)

    fa.foldMapLeft1(bifpoint[EitherT[G, ?, ?], F, A, B]) { (b, a) =>
      EitherT(b.validation.flatMap { bv =>
        a.validation.map { av =>
          (biIdT(bv) +++ bifpoint[Validation, IdT[F, ?], A, B](av)).disjunction.bimap(_.run, _.run)
        }
      })
    }
  }

  def mapT[F[_], G[_], A, B, C, D](fa: EitherT[F, A, B], f: F[A \/ B] => G[C \/ D]): EitherT[G, C, D] =
    EitherT(f(fa.run))

  def mapF[F[_], G[_], A, B](fa: EitherT[F, A, B], f: F[A \/ B] => G[A \/ B]): EitherT[G, A, B] =
    mapT(fa, f)

  def head1[F[_]: Foldable1, A](fa: F[A]): A =
    fa.foldMapRight1(identity[A]) { (a, _) => a }

  def untilFirstLeft[F[_]: Foldable1: Applicative: Plus, G[_]: Monad, A, B, E](fa: F[A], f: A => EitherT[G, E, B]): EitherT[G, (E, F[A]), F[B]] = {
    type S[X] = StateT[G, Option[E], X]

    def run(a: A): G[(Option[E], (E, A) \/ B)] =
      f(a).run.map(e => (e.swap.toOption, e.leftMap((_, a))))

    val eitherts: F[EitherT[S, (E, A), B]] =
      fa.map { a =>
        EitherT[S, (E, A), B](
          StateT { firstError =>
            firstError.fold(run(a)) { e =>
              (e.some, (e, a).left[B]).point[G]
            }
          }
        )
      }

    mapF[S, G, F[(E, A)], F[B]](
      eitherTSeparate1(eitherts),
      _.eval(None)
    ).leftMap { as =>
        (head1(as)._1, as.map(_._2))
      }
  }
}
