package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ -\/, Contravariant, Monad, NonEmptyList, \/- }
import scalaz.syntax.either._
import scalaz.syntax.monad._

case class SaveAPIConfig[F[_]](retry: RetryStrategy[F])

object SaveAPIConfig {
  def default[F[_]: Monad: LiftIO] =
    SaveAPIConfig(RetryStrategy.retryIntervals(
      RetryInterval.fullJitter(20, 5.millis, 2.0), Delays.sleep
    ))
}

sealed trait SaveAPI[F[_], K, S, E] { self =>
  def save(config: SaveAPIConfig[F])(key: K, operation: Operation[S, E]): F[SaveResult[S]]

  /** contramap on the key type */
  def contramap[KK](f: KK => K): SaveAPI[F, KK, S, E]
}

object SaveAPI {
  def apply[F[_]: LiftIO: Monad, K, S: Sequence, E](store: EventStorage[F, K, S, E]): SaveAPI[F, K, S, E] =
    new Impl(identity, store)

  implicit def SaveAPIContravariant[F[_], S, E]: Contravariant[SaveAPI[F, ?, S, E]] =
    new Contravariant[SaveAPI[F, ?, S, E]] {
      override def contramap[K, KK](api: SaveAPI[F, K, S, E])(f: KK => K): SaveAPI[F, KK, S, E] =
        api.contramap(f)
    }

  private[SaveAPI] class Impl[F[_]: LiftIO: Monad, K, S: Sequence, E, IK](
      toStreamKey: K => IK,
      store: EventStorage[F, IK, S, E]
  ) extends SaveAPI[F, K, S, E] {
    override def save(config: SaveAPIConfig[F])(key: K, operation: Operation[S, E]): F[SaveResult[S]] =
      Retry[F, SaveResult[S]](doSave(key, operation), config.retry, _.fold(_ => false, _ => false, true))

    def contramap[KK](f: KK => K): SaveAPI[F, KK, S, E] =
      new Impl[F, KK, S, E, IK](f andThen toStreamKey, store)

    private def doSave(key: K, operation: Operation[S, E])(retryCount: Int): F[SaveResult[S]] =
      for {
        seq <- store.latest(toStreamKey(key)).map { _.id.seq }.run
        result <- operation.apply(seq).fold(
          EventStreamError.reject(_).left[Event[IK, S, E]].point[F],
          e => store.put(Event.next[IK, S, E](toStreamKey(key), seq, e))
        )
      } yield result match {
        case -\/(EventStreamError.DuplicateEvent)     => SaveResult.timedOut[S](retryCount)
        case -\/(EventStreamError.Rejected(r))        => SaveResult.reject[S](r, retryCount)
        case -\/(EventStreamError.EventNotFound)      => SaveResult.reject[S](NonEmptyList(Reason("The original event changed")), retryCount)
        case -\/(EventStreamError.EventIdsDoNotMatch) => SaveResult.reject[S](NonEmptyList(Reason("The supplied events didn't have matching IDs")), retryCount)
        case \/-(event)                               => SaveResult.success[S](event.id.seq, retryCount)
      }
  }
}
