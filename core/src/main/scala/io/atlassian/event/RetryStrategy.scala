package io.atlassian.event

import scala.concurrent.duration.Duration
import scalaz.effect.{ IO, LiftIO }
import scalaz.{ Monad, \/ }
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

trait RetryStrategy[F[_]] {
  def tryRun[X](a: F[X], retriable: X => Boolean): F[RetryStrategy[F] \/ X]
}

object RetryStrategy {
  def durationList[F[_]: Monad: LiftIO](tries: List[Duration], delay: Duration => IO[Unit]): RetryStrategy[F] =
    new RetryStrategy[F] {
      def tryRun[X](a: F[X], retriable: X => Boolean): F[RetryStrategy[F] \/ X] =
        for {
          x <- a
          afterDelay <- if (!retriable(x)) x.right[RetryStrategy[F]].point[F]
          else {
            tries match {
              case d :: ds =>
                LiftIO[F].liftIO {
                  delay(d)
                } >> durationList(ds, delay).left.point[F]
              case _ => x.right[RetryStrategy[F]].point[F]
            }
          }
        } yield afterDelay
    }

  def retryIntervals[F[_]: Monad: LiftIO](tries: RetryInterval, delay: Duration => IO[Unit]): RetryStrategy[F] =
    new RetryStrategy[F] {
      def tryRun[X](a: F[X], retriable: X => Boolean): F[RetryStrategy[F] \/ X] =
        for {
          x <- a
          afterDelay <- if (!retriable(x)) x.right[RetryStrategy[F]].point[F]
          else {
            LiftIO[F].liftIO {
              for {
                next <- tries.next
                result <- next.some {
                  case (d, nextInterval) =>
                    delay(d).map { _ => retryIntervals(nextInterval, delay).left[X] }
                }.none { x.right[RetryStrategy[F]].point[IO] }
              } yield result
            }
          }
        } yield afterDelay
    }
}

object Delays {
  val sleep: Duration => IO[Unit] =
    d => IO { Thread.sleep(d.toMillis) }
}