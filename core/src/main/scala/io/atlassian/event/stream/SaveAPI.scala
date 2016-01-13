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
      RetryIntervals.fullJitter(20, 5.millis, 2.0), Delays.sleep
    ))
}

case class SaveAPI[F[_]: LiftIO, KK, E, K, S](
    toStreamKey: K => KK,
    eventStore: EventStorage[F, KK, S, E]
) {

  def save(config: SaveAPIConfig[F])(key: K, operation: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    Retry[F, SaveResult[S]](doSave(key, operation), config.retry, _.fold(_ => false, _ => false, true))

  private def doSave(key: K, operation: Operation[S, E])(implicit M: Monad[F], T: Sequence[S]): F[SaveResult[S]] =
    for {
      seq <- eventStore.latest(toStreamKey(key)).map { _.id.seq }.run
      result <- operation.apply(seq).fold(
        EventStreamError.reject(_).left[Event[KK, S, E]].point[F],
        e => eventStore.put(Event.next[KK, S, E](toStreamKey(key), seq, e))
      )
      transform <- result match {
        case -\/(EventStreamError.DuplicateEvent) => SaveResult.timedOut[S].point[F]
        case -\/(EventStreamError.Rejected(r))    => SaveResult.reject[S](r).point[F]
        case \/-(event)                           => SaveResult.success[S](event.id.seq).point[F]
      }
    } yield transform
}
