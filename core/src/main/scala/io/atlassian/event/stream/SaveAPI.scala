package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ Bind, Foldable, Foldable1, Functor, Monad, Monoid, NonEmptyList }
import scalaz.std.list._
import scalaz.syntax.all._
import scalaz.syntax.std.option._

trait SaveAPI[F[_], KK, E, K, S] {
  def save(key: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]

  def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]
}

object SaveAPI {

  def foldMapLeft1M[F[_]: Foldable1, G[_]: Bind, A, B](fa: F[A])(z: A => G[B])(f: (B, A) => G[B]): G[B] =
    fa.foldMapLeft1(z) { (b, a) =>
      b.flatMap(f(_, a))
    }

  def handleEventStreamError[A](retryCount: Int)(e: EventStreamError): SaveResult[A] =
    e match {
      case EventStreamError.DuplicateEvent => SaveResult.timedOut[A](retryCount)
      case EventStreamError.Rejected(r)    => SaveResult.reject[A](r, retryCount)
    }

  def canRetryResult[A](r: SaveResult[A]): Boolean =
    r.fold(_ => false, _ => false, true)

  def operationResultToSaveResult[A](retryCount: Int)(r: Operation.Result[A]): SaveResult[A] =
    r.fold(SaveResult.Reject(_, retryCount), SaveResult.Success(_, retryCount))

  def runOperations[KK, S: Sequence, E](key: KK, latest: S, ops: NonEmptyList[Operation[S, E]]): Operation.Result[NonEmptyList[Event[KK, S, E]]] = {
    def event(s: S, e: E): Event[KK, S, E] =
      Event.next(key, Some(s), e)

    foldMapLeft1M(ops)(_(latest).map(e => NonEmptyList(event(latest, e)))) { (nel, op) =>
      val s = nel.head.id.seq
      op(s).map(event(s, _) <:: nel)
    }.map(_.reverse)
  }

  def apply[F[_]: LiftIO, KK, E, K, S](config: SaveAPI.Config[F], toStreamKey: K => KK, store: EventStorage[F, KK, S, E]): SaveAPI[F, KK, E, K, S] =
    new SaveAPI[F, KK, E, K, S] {
      override def save(k: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
        batch(k, NonEmptyList(op))

      override def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def go(retryCount: Int): F[SaveResult[S]] =
          latestSeq(key).flatMap { latest =>
            val s = latest.getOrElse(Sequence[S].first)
            runOperations(toStreamKey(key), s, ops).traverse(store.batchPut[NonEmptyList]).map {
              operationResultToSaveResult(retryCount)(_).flatMap {
                _.fold(
                  handleEventStreamError(retryCount),
                  events => SaveResult.Success(events.last.id.seq, retryCount)
                )
              }
            }
          }

        Retry(go, config.retry, canRetryResult)
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
