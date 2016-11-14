package io.atlassian.event

import scalaz.{ \/-, -\/, Monad }
import scalaz.syntax.monad._

object Retry {
  def apply[F[_]: Monad, A](f: Int => F[A], strategy: RetryStrategy[F], retriable: A => Boolean): F[A] = {
    def doRetry(retry: RetryStrategy[F], retryCount: Int): F[A] = {
      retry(f(retryCount), retriable) >>= {
        case -\/(newRetry) => doRetry(newRetry, retryCount + 1)
        case \/-(result)   => result.point[F]
      }
    }

    doRetry(strategy, 0)
  }
}
