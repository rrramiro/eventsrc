package io.atlassian.event
package stream

import scala.concurrent.duration._
import scalaz.effect.LiftIO
import scalaz.{ Foldable, Functor, Monad, Monoid, NonEmptyList }
import scalaz.syntax.all._
import scalaz.syntax.std.option._

trait SaveAPI[F[_], KK, E, K, S] {
  def save(key: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]

  def batch(key: K, ops: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]]
}

object SaveAPI {

  def apply[F[_]: LiftIO, KK, E, K, S](config: SaveAPI.Config[F], toStreamKey: K => KK, store: EventStorage[F, KK, S, E]): SaveAPI[F, KK, E, K, S] =
    new SaveAPI[F, KK, E, K, S] {
      override def save(k: K, op: Operation[S, E])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
        batch(k, NonEmptyList(op))
      //unsafe(k, op)(latestSeq(k))

      override def batch(k: K, startOps: NonEmptyList[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] = {
        def go(seq: F[Option[S]], ops: List[Operation[S, E]], acc: F[SaveResult[S]]): F[SaveResult[S]] =
          ops match {
            case op :: t => unsafe(k, op)(seq) >>= {
              case result @ SaveResult.Success(nextSeq, retryCount) => go(nextSeq.some.point[F], t, acc.map { _ |+| result })
              case fail @ _                                         => fail.point[F]
            }
            case Nil => acc
          }
        go(latestSeq(k), startOps.toList, Monoid[SaveResult[S]].zero.point[F])
      }

      def batch2[G[_]: Foldable](key: K, ops: G[Operation[S, E]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
        latestSeq(key) >>= { prevSeq =>
          ops.foldLeft[F[(Option[S], SaveResult[S])]] { (prevSeq, Monoid[SaveResult[S]].zero).point[F] } {
            case (as, op) =>
              as >>= { case (seq, a) => unsafe(key, op)(seq.point[F]).map { b => (seq.map { S.next }, a |+| b) } }
          }.map { case (_, result) => result }
        }

      private def latestSeq(key: K)(implicit F: Functor[F]): F[Option[S]] =
        store.latest(toStreamKey(key)).map { _.id.seq }.run

      private def unsafe(key: K, op: Operation[S, E])(prev: F[Option[S]])(implicit F: Monad[F], S: Sequence[S]): F[SaveResult[S]] =
        Retry[F, SaveResult[S]](
          retryCount =>
            for {
              seq <- prev
              result <- op(seq).fold(
                EventStreamError.reject(_).left[Event[KK, S, E]].point[F],
                e => store.put(Event.next[KK, S, E](toStreamKey(key), seq, e))
              )
            } yield result.fold(
              {
                case EventStreamError.DuplicateEvent => SaveResult.timedOut[S](retryCount)
                case EventStreamError.Rejected(r)    => SaveResult.reject[S](r, retryCount)
              },
              event => SaveResult.success[S](event.id.seq, retryCount)
            ),
          config.retry,
          _.fold(_ => false, _ => false, true)
        )
    }

  case class Config[F[_]](retry: RetryStrategy[F])
  object Config {
    def default[F[_]: Monad: LiftIO] =
      Config(RetryStrategy.retryIntervals(
        RetryInterval.fullJitter(20, 2.millis, 2.0), Delays.sleep
      ))
  }
}
