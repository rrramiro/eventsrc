package io.atlassian.event

import scala.concurrent.duration.Duration
import scalaz.{ Monad, \/ }
import scalaz.syntax.either._
import scalaz.syntax.monad._

trait RetryStrategy[F[_]] {
  def tryRun[X](a: F[X], retriable: X => Boolean): F[X \/ RetryStrategy[F]]
}

object RetryStrategy {
  def apply[F[_]: Monad](tries: Seq[Duration]): RetryStrategy[F] =
    new RetryStrategy[F] {
      def tryRun[X](a: F[X], retriable: X => Boolean): F[X \/ RetryStrategy[F]] =
        for {
          x <- a
          afterDelay <- if (!retriable(x)) x.left[RetryStrategy[F]].point[F]
          else {
            tries match {
              case d :: ds =>
                Thread.sleep(d.toMillis)
                apply(ds).right.point[F]
              case _ => x.left[RetryStrategy[F]].point[F]
            }
          }
        } yield afterDelay
    }
}
