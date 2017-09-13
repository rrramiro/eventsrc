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
        case -\/(EventStreamError.DuplicateEvent) => SaveResult.timedOut[S](retryCount)
        case -\/(EventStreamError.Rejected(r))    => SaveResult.reject[S](r, retryCount)
        case \/-(event)                           => SaveResult.success[S](event.id.seq, retryCount)
      }

      type UnprocessedResult = (SaveResult[S], NonEmptyList[Operation[S, E]])
      type PartialResult = UnprocessedResult \/ SaveResult[S]

      override def batch(key: K, initialOps: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def go(ops: Option[PartialResult], retryCount: Int): F[PartialResult] =
          latestSeq(key).flatMap { latest =>
            storeOperations(store, toStreamKey(key), latest.getOrElse(Sequence[S].first), ops.flatMap(unprocessedOps).getOrElse(initialOps), retryCount)
          }

        def latestSeq(key: K)(implicit F: Functor[F]): F[Option[S]] =
          store.latest(toStreamKey(key)).map { _.id.seq }.run

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
                  EitherT(store.put(event)).bimap(
                    SaveResult.fromEventStreamError(retryCount),
                    e => SaveResult.success(e.id.seq, retryCount)
                  )
              }).bimap(toUnprocessedResult, _.last)
            } yield result

          program.run
        }

        Retry[F, PartialResult](go, config.retry, partialSaveResult(_).canRetry).map(partialSaveResult)
      }
  }
}
