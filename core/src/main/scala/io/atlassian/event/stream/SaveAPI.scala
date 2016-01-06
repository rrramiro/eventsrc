package io.atlassian.event
package stream

import java.util.concurrent.ScheduledExecutorService

import scala.concurrent.duration._
import scalaz.{ Monad, NonEmptyList, ~>, \/-, -\/ }
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.concurrent.Task

case class SaveAPIConfig(retry: Retry, executor: ScheduledExecutorService)

object SaveAPIConfig {
  def default(executor: ScheduledExecutorService) = SaveAPIConfig(Retry.fullJitter(20, 5.millis, 2.0), executor)
}

case class SaveAPI[F[_], KK, E, K, S](
    taskToF: Task ~> F,
    toStreamKey: K => KK,
    eventStore: EventStorage[F, KK, S, E]
) {

  def save(config: SaveAPIConfig)(key: K, operation: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    saveWithRetry(key, operation, Seq(0.milli) ++ config.retry.run, config.executor)

  // TODO: Maybe just make a MonadTask trait and use it as a constraint.
  private def saveWithRetry(key: K, operation: Operation[S, E], durations: Seq[Duration], executor: ScheduledExecutorService)(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    durations match {
      case d :: ds =>
        for {
          _ <- taskToF {
            if (d.toMillis == 0)
              Task.now(())
            else
              Task.schedule((), d)(executor)
          }
          seq <- eventStore.latest(toStreamKey(key)).map { _.id.seq }.run
          result <- operation.apply(seq).fold(
            EventStreamError.reject(_).left[Event[KK, S, E]].point[F],
            e => eventStore.put(Event.next[KK, S, E](toStreamKey(key), seq, e))
          )
          transform <- result match {
            case -\/(EventStreamError.DuplicateEvent) => saveWithRetry(key, operation, ds, executor)
            case -\/(EventStreamError.Rejected(r))    => SaveResult.reject[S](r).point[F]
            case \/-(event)                           => SaveResult.success[S](event.id.seq).point[F]
          }
        } yield transform

      case _ => SaveResult.reject[S](NonEmptyList(Reason("Failed to save after retries"))).point[F]
    }
}
