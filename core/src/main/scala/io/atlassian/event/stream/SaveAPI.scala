package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.{ Monad, NonEmptyList, \/-, -\/ }
import scalaz.syntax.either._
import scalaz.syntax.monad._

case class SaveAPIConfig(retry: Retry)

object SaveAPIConfig {
  val default = SaveAPIConfig(Retry.fullJitter(20, 5.millis, 2.0))
}

case class SaveAPI[F[_], KK, E, K, S](
    toStreamKey: K => KK,
    eventStore: EventStorage[F, KK, S, E]
) {

  def save(config: SaveAPIConfig)(key: K, operation: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    saveWithRetry(key, operation, Seq(0.milli) ++ config.retry.run)

  private def saveWithRetry(key: K, operation: Operation[S, E], durations: Seq[Duration])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    durations match {
      case d :: ds =>
        for {
          _ <- { if (d.toMillis > 0) Thread.sleep(d.toMillis) else () }.point[F]
          seq <- eventStore.latest(toStreamKey(key)).map { _.id.seq }.run
          result <- operation.apply(seq).fold(
            EventStreamError.reject(_).left[Event[KK, S, E]].point[F],
            e => eventStore.put(Event.next[KK, S, E](toStreamKey(key), seq, e))
          )
          transform <- result match {
            case -\/(EventStreamError.DuplicateEvent) => saveWithRetry(key, operation, ds)
            case -\/(EventStreamError.Rejected(r))    => SaveResult.reject[S](r).point[F]
            case \/-(event)                           => SaveResult.success[S](event.id.seq).point[F]
          }
        } yield transform

      case _ => SaveResult.reject[S](NonEmptyList(Reason("Failed to save after retries"))).point[F]
    }
}
