package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ Monad, \/-, -\/ }
import scalaz.syntax.either._
import scalaz.syntax.monad._

case class SaveAPIConfig[F[_]](retry: RetryStrategy[F])

object SaveAPIConfig {
  def default[F[_]: Monad: LiftIO] =
    SaveAPIConfig(RetryStrategy.retryIntervals(
      RetryInterval.fullJitter(20, 5.millis, 2.0), Delays.sleep
    ))
}

trait SaveAPI[F[_], KK, E, K, S] {
  def save(key: K, operation: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]
}

object SaveAPI {
  def apply[F[_]: LiftIO, KK, E, K, S](config: SaveAPIConfig[F], toStreamKey: K => KK, store: EventStorage[F, KK, S, E]): SaveAPI[F, KK, E, K, S] =
    new SaveAPI[F, KK, E, K, S] {
      def save(key: K, operation: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def doSave(key: K, operation: Operation[S, E])(retryCount: Int): F[SaveResult[S]] =
          for {
            seq <- store.latest(toStreamKey(key)).map { _.id.seq }.run
            result <- operation.apply(seq).fold(
              EventStreamError.reject(_).left[Event[KK, S, E]].point[F],
              e => store.put(Event.next[KK, S, E](toStreamKey(key), seq, e))
            )
          } yield result.fold(
            {
              case EventStreamError.DuplicateEvent => SaveResult.timedOut[S](retryCount)
              case EventStreamError.Rejected(r)    => SaveResult.reject[S](r, retryCount)
            },
            event => SaveResult.success[S](event.id.seq, retryCount)
          )

        Retry[F, SaveResult[S]](doSave(key, operation), config.retry, _.fold(_ => false, _ => false, true))
      }
    }
}
