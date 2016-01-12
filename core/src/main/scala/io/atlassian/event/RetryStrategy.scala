package io.atlassian.event

import scala.concurrent.duration.Duration
import scalaz.{ Monad, \/, -\/, \/- }
import scalaz.syntax.either._
import scalaz.syntax.monad._

trait RetryStrategy[F[_]] {
  def tryRun[X](a: F[X], retriable: X => Boolean): F[RetryStrategy[F] \/ X]
}

object RetryStrategy {
  def apply[F[_]: Monad](tries: Seq[Duration]): RetryStrategy[F] =
    new RetryStrategy[F] {
      def tryRun[X](a: F[X], retriable: X => Boolean): F[RetryStrategy[F] \/ X] =
        for {
          x <- a
          afterDelay <- if (!retriable(x)) x.right[RetryStrategy[F]].point[F]
          else {
            tries match {
              case d :: ds =>
                Thread.sleep(d.toMillis)
                apply(ds).left.point[F]
              case _ => x.right[RetryStrategy[F]].point[F]
            }
          }
        } yield afterDelay
    }

  def apply[F[_]: Monad](tries: RetryIntervals): RetryStrategy[F] =
    apply(tries.unsafePerformIO)
}

