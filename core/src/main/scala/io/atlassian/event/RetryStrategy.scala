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
      def tryRun[A](fa: F[A], retriable: A => Boolean): F[RetryStrategy[F] \/ A] =
        fa >>= { a =>
          if (!retriable(a)) a.right[RetryStrategy[F]].point[F]
          else tries match {
            case d :: ds => LiftIO[F].liftIO {
              delay(d)
            }.map { _ => durationList(ds, delay).left }
            case _ => a.right[RetryStrategy[F]].point[F]
          }
        }
    }

  def retryIntervals[F[_]: Monad: LiftIO](tries: RetryInterval, delay: Duration => IO[Unit]): RetryStrategy[F] =
    new RetryStrategy[F] {
      def tryRun[A](fa: F[A], retriable: A => Boolean): F[RetryStrategy[F] \/ A] =
        fa >>= { a =>
          if (!retriable(a)) a.right[RetryStrategy[F]].point[F]
          else LiftIO[F].liftIO {
            tries.next >>= {
              _.some {
                case (d, next) => delay(d).map { _ => retryIntervals(next, delay).left[A] }
              }.none { a.right[RetryStrategy[F]].point[IO] }
            }
          }
        }
    }
}

object Delays {
  val sleep: Duration => IO[Unit] =
    d => IO { Thread.sleep(d.toMillis) }
}
