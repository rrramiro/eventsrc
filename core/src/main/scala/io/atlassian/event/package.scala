package io.atlassian

import scalaz._

package object event {
  implicit class TaggedOps[A, T](val a: A @@ T) extends AnyVal {
    def unwrap: A = Tag.unwrap(a)
  }

  def foldMapLeft1M[F[_], G[_], A, B](fa: F[A])(z: A => G[B])(f: (B, A) => G[B])(implicit F: Foldable1[F], G: Bind[G]): G[B] =
    F.foldMapLeft1(fa)(z) { (b, a) =>
      G.bind(b)(f(_, a))
    }

}
