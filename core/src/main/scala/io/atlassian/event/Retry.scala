package io.atlassian.event

import scalaz.{ \/-, -\/, Monad }
import scalaz.syntax.monad._

object Retry {
  def apply[F[_]: Monad, A](f: (Option[A], Int) => F[A], strategy: RetryStrategy[F], retriable: A => Boolean): F[A] = {
    def doRetry(retry: RetryStrategy[F], lastResult: Option[A], retryCount: Int): F[A] = {
      val result = f(lastResult, retryCount)
      retry(result, retriable) >>= {
        case -\/(newRetry) => doRetry(newRetry, lastResult, retryCount + 1)
        case \/-(result)   => result.point[F]
      }
    }

    doRetry(strategy, None, 0)
  }
}
