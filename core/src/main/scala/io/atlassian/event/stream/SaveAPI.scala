package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ Applicative, Functor, Monad, NonEmptyList }
import scalaz.syntax.all._

trait SaveAPI[F[_], KK, E, K, S] {
  def save(key: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]

  def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]
}

object SaveAPI {

  def storeOperations[F[_]: Applicative, KK, K, S: Sequence, E](store: EventStorage[F, KK, S, E], key: KK, s: S, ops: NonEmptyList[Operation[S, E]], retryCount: Int): F[SaveResult[S]] =
    Operation.runMany(key, s, ops).traverse(store.batchPut[NonEmptyList]).map {
      SaveResult.fromOperationResult(retryCount)(_).flatMap {
        _.fold(
          SaveResult.fromEventStreamError(retryCount),
          events => SaveResult.Success(events.last.id.seq, retryCount)
        )
      }
    }

  def apply[F[_]: LiftIO, KK, E, K, S](config: SaveAPI.Config[F], toStreamKey: K => KK, store: EventStorage[F, KK, S, E]): SaveAPI[F, KK, E, K, S] =
    new SaveAPI[F, KK, E, K, S] {
      override def save(k: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
        batch(k, NonEmptyList(op))

      override def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def go(retryCount: Int): F[SaveResult[S]] =
          latestSeq(key).flatMap { latest =>
            storeOperations(store, toStreamKey(key), latest.getOrElse(Sequence[S].first), ops, retryCount)
          }

        Retry(go, config.retry, _.canRetry)
      }

      private def latestSeq(key: K)(implicit F: Functor[F]): F[Option[S]] =
        store.latest(toStreamKey(key)).map { _.id.seq }.run
    }

  case class Config[F[_]](retry: RetryStrategy[F])
  object Config {
    def default[F[_]: Monad: LiftIO] =
      Config(RetryStrategy.retryIntervals[F](
        RetryInterval.fullJitter(20, 2.millis, 2.0), Delays.sleep
      ))
  }
}
