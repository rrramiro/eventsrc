package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ \/, Applicative, EitherT, Foldable1, Functor, Monad, NonEmptyList, Plus, StateT }
import scalaz.std.tuple._
import scalaz.syntax.all._

trait SaveAPI[F[_], KK, E, K, S] {
  final def save(key: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    batch(key, NonEmptyList(op))

  def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]
}

object SaveAPI {

  type UnprocessedResult[S, E] = (SaveResult[S], NonEmptyList[Operation[S, E]])
  type PartialResult[S, E] = UnprocessedResult[S, E] \/ SaveResult[S]

  def partialSaveResult[S, E, A](p: PartialResult[S, E]): SaveResult[S] =
    p.fold(_._1, identity)

  def unprocessedOps[S, E](p: PartialResult[S, E]): Option[NonEmptyList[Operation[S, E]]] =
    p.fold(a => Some(a._2), _ => None)

  def storeOperations[F[_]: Monad: LiftIO, KK, K, S: Sequence, E](store: EventStorage[F, KK, S, E], key: KK, s: S, ops: NonEmptyList[Operation[S, E]], retryCount: Int): F[PartialResult[S, E]] = {
    def handleOperationResult[A](a: Operation.Result[A]): EitherT[F, UnprocessedResult[S, E], A] =
      EitherT(a.toDisjunction.point[F]).leftMap(a => (SaveResult.reject[S](a, retryCount), ops))

    def toUnprocessedResult[A](u: (SaveResult[S], NonEmptyList[(Operation[S, E], A)])): UnprocessedResult[S, E] =
      u.rightMap(_.map(_._1))

    val program =
      for {
        enumerated <- LiftIO[({ type l[a] = EitherT[F, UnprocessedResult[S, E], a] })#l].liftIO(Operation.enumerate(key, s, ops))
        events <- handleOperationResult(enumerated)
        result <- untilFirstLeft[NonEmptyList, F, (Operation[S, E], Event[KK, S, E]), SaveResult[S], SaveResult[S]](ops.zip(events), {
          case (_, event) =>
            store.put(event).map(
              _.bimap(
                SaveResult.fromEventStreamError(retryCount),
                e => SaveResult.success(e.id.seq, retryCount)
              )
            )
        }).bimap(toUnprocessedResult, _.head)
      } yield result

    program.run
  }

  def apply[F[_]: LiftIO, KK, E, K, S](config: SaveAPI.Config[F], toStreamKey: K => KK, store: EventStorage[F, KK, S, E]): SaveAPI[F, KK, E, K, S] =
    new SaveAPI[F, KK, E, K, S] {
      override def batch(key: K, initialOps: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def go(ops: Option[PartialResult[S, E]], retryCount: Int): F[PartialResult[S, E]] =
          latestSeq(key).flatMap { latest =>
            storeOperations(store, toStreamKey(key), latest.getOrElse(Sequence[S].first), ops.flatMap(unprocessedOps).getOrElse(initialOps), retryCount)
          }

        Retry[F, PartialResult[S, E]](go, config.retry, partialSaveResult(_).canRetry).map(partialSaveResult)
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
