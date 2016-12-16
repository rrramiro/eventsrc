package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ \/, EitherT, Functor, Monad, NonEmptyList }
import scalaz.std.tuple._
import scalaz.syntax.all._

trait SaveAPI[F[_], KK, E, K, S] {
  final def save(key: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
    batch(key, NonEmptyList(op))

  def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]
}

object SaveAPI {

  def apply[F[_]: LiftIO, KK, E, K, S](config: SaveAPI.Config[F], toStreamKey: K => KK, store: EventStorage[F, KK, S, E]): SaveAPI[F, KK, E, K, S] =
    new SaveAPI[F, KK, E, K, S] {
      type UnprocessedResult = (SaveResult[S], NonEmptyList[Operation[S, E]])
      type PartialResult = UnprocessedResult \/ SaveResult[S]

      override def batch(key: K, initialOps: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def go(ops: Option[PartialResult], retryCount: Int): F[PartialResult] =
          latestSeq(key).flatMap { latest =>
            storeOperations(store, toStreamKey(key), latest.getOrElse(Sequence[S].first), ops.flatMap(unprocessedOps).getOrElse(initialOps), retryCount)
          }

        def partialSaveResult[A](p: PartialResult): SaveResult[S] =
          p.fold(_._1, identity)

        def unprocessedOps(p: PartialResult): Option[NonEmptyList[Operation[S, E]]] =
          p.fold(a => Some(a._2), _ => None)

        def storeOperations(store: EventStorage[F, KK, S, E], key: KK, s: S, ops: NonEmptyList[Operation[S, E]], retryCount: Int): F[PartialResult] = {
          def handleOperationResult[A](a: Operation.Result[A]): EitherT[F, UnprocessedResult, A] =
            EitherT(a.toDisjunction.point[F]).leftMap(a => (SaveResult.reject[S](a, retryCount), ops))

          def toUnprocessedResult[A](u: (SaveResult[S], NonEmptyList[(Operation[S, E], A)])): UnprocessedResult =
            u.rightMap(_.map(_._1))

          val program =
            for {
              enumerated <- LiftIO[EitherT[F, UnprocessedResult, ?]].liftIO(Operation.enumerate(key, s, ops))
              events <- handleOperationResult(enumerated)
              result <- untilFirstLeft[NonEmptyList, F, (Operation[S, E], Event[KK, S, E]), SaveResult[S], SaveResult[S]](ops.zip(events), {
                case (_, event) =>
                  store.put(event).map(
                    _.bimap(
                      SaveResult.fromEventStreamError(retryCount),
                      e => SaveResult.success(e.id.seq, retryCount)
                    )
                  )
              }).bimap(toUnprocessedResult, _.last)
            } yield result

          program.run
        }

        Retry[F, PartialResult](go, config.retry, partialSaveResult(_).canRetry).map(partialSaveResult)
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
