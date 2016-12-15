package io.atlassian

import scalaz._
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._
import scalaz.syntax.either._
import scalaz.syntax.foldable1._
import scalaz.syntax.monad._
import scalaz.syntax.plus._

package object event {
  implicit class TaggedOps[A, T](val a: A @@ T) extends AnyVal {
    def unwrap: A = Tag.unwrap(a)
  }

  def foldMapLeft1M[F[_], G[_], A, B](fa: F[A])(z: A => G[B])(f: (B, A) => G[B])(implicit F: Foldable1[F], G: Bind[G]): G[B] =
    F.foldMapLeft1(fa)(z) { (b, a) =>
      G.bind(b)(f(_, a))
    }

  def untilFirstLeft[F[_]: Foldable1: Applicative: Plus, G[_]: Monad, A, B, C](fa: F[A], f: A => G[B \/ C]): EitherT[G, (B, F[A]), F[C]] = {
    def go(a: A): G[(B, F[A]) \/ F[C]] =
      f(a).map(
        _.bimap(
          (_, a.point[F]),
          _.point[F]
        )
      )

    EitherT(fa.foldMapLeft1(go) { (g, a) =>
      g.flatMap {
        _.fold(
          _.rightMap(_ <+> a.point[F]).left.point[G],
          cs => go(a).map(_.rightMap(cs <+> _))
        )
      }
    })
  }

}
