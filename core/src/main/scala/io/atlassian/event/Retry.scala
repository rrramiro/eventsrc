package io.atlassian.event

import scalaz.{ \/-, -\/, Monad }
import scalaz.syntax.monad._

object Retry {
  def apply[F[_]: Monad, A](f: => F[A], strategy: RetryStrategy[F], retriable: A => Boolean): F[A] =
    strategy.tryRun(f, retriable) flatMap {
      case -\/(retry)  => apply(f, retry, retriable)
      case \/-(result) => result.point[F]
    }
}
