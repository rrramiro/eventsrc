package io.atlassian.event
package stream

import org.joda.time.DateTime

import scala.concurrent.duration._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{ process1, Process }
import scalaz.syntax.all._
import scalaz.syntax.std.option._

/**
 * EventStreamError represents any error conditions that are useful to represent for event sources. In particular,
 * we need to know about attempts to store duplicate events.
 */
sealed trait EventStreamError
object EventStreamError {
  def noop: EventStreamError = Noop

  def reject(s: NonEmptyList[Reason]): EventStreamError = Rejected(s)

  val duplicate: EventStreamError = DuplicateEvent

  case object DuplicateEvent extends EventStreamError

  case class Rejected(s: NonEmptyList[Reason]) extends EventStreamError

  // if the client rejects an update operation
  case object Noop extends EventStreamError
}

sealed trait QueryConsistency {
  import QueryConsistency._

  def fold[X](snapshot: => X, event: => X): X =
    this match {
      case LatestSnapshot => snapshot
      case LatestEvent => event
    }
}
object QueryConsistency {
  case object LatestSnapshot extends QueryConsistency
  case object LatestEvent extends QueryConsistency

  val latestSnapshot: QueryConsistency = LatestSnapshot
  val latestEvent: QueryConsistency = LatestEvent
}

case class LatestSnapshotResult[S, V](latest: Snapshot[S, V], previousPersisted: Snapshot[S, V])

case class QueryAPI[F[_], KK, E, K, S, V](
    toStreamKey: K => KK,
    eventStore: EventStorage[F, KK, S, E],
    snapshotStore: SnapshotStorage[F, K, S, V],
    acc: K => (Snapshot[S, V], Event[KK, S, E]) => Snapshot[S, V]
) {
  /**
   * Return the current view of the data for key 'key'
   */
  def get(key: K, consistency: QueryConsistency)(implicit F: Monad[F], FC: Catchable[F]): F[Option[V]] =
    getSnapshot(key, consistency).map { _.value }

  /**
   * @return the current view wrapped in Snapshot of the data for key 'key'
   */
  def getSnapshot(key: K, consistency: QueryConsistency)(implicit F: Monad[F], FC: Catchable[F]): F[Snapshot[S, V]] =
    consistency.fold(
      snapshotStore.get(key, SequenceQuery.latest[S]),
      for {
        latestSnapshot <- generateLatestSnapshot(key)
        _ = persistSnapshot(key, latestSnapshot.latest, latestSnapshot.previousPersisted.some)
      } yield latestSnapshot.latest
    )

  /**
   * Generates the latest snapshot by retrieving the last persisted snapshot and then replaying events on top of that.
   */
  def generateLatestSnapshot(key: K)(implicit F: Monad[F], FC: Catchable[F]): F[LatestSnapshotResult[S, V]] =
    for {
      persistedSnapshot <- snapshotStore.get(key, SequenceQuery.latest[S])
      fromSeq = persistedSnapshot.seq
      events = eventStore.get(toStreamKey(key), fromSeq)
      theSnapshot <- snapshotFold(persistedSnapshot, events, acc(key))
    } yield LatestSnapshotResult(theSnapshot, persistedSnapshot)

  /**
   * @param key The key of the aggregate to retrieve
   * @param at If none, get me the latest. If some, get me the snapshot at that specific sequence.
   * @return A Snapshot for the aggregate at the given sequence number.
   */
  private def generateSnapshotAt(key: K, at: Option[S])(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]): F[Snapshot[S, V]] =
    for {
      persistedSnapshot <- snapshotStore.get(key, at.fold(SequenceQuery.latest[S])(SequenceQuery.before))
      fromSeq = persistedSnapshot.seq
      pred = at.fold[Event[KK, S, E] => Boolean] { _ => true } { seq => e => e.id.seq <= seq }
      events = eventStore.get(toStreamKey(key), fromSeq).takeWhile(pred)
      theSnapshot <- snapshotFold(persistedSnapshot, events, acc(key))
    } yield theSnapshot

  /**
   * Return the view of the data for the key 'key' at the specified sequence number.
   * @param key the key
   * @param seq the sequence number of the event at which we want the see the view of the data.
   * @return view of the data at event with sequence 'seq'
   */
  def getAt(key: K, seq: S)(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]): F[Option[V]] =
    generateSnapshotAt(key, Some(seq)).map { _.value }

  /**
   * Get a stream of Snapshots starting from sequence number 'from' (if defined).
   * @param key The key
   * @param from Starting sequence number. None to get from the beginning of the stream.
   * @return a stream of Snapshots starting from sequence number 'from' (if defined).
   */
  def getHistory(key: K, from: Option[S])(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]): F[Process[F, Snapshot[S, V]]] =
    for {
      startingSnapshot <- generateSnapshotAt(key, from)
    } yield eventStore.get(toStreamKey(key), startingSnapshot.seq)
      .scan[Snapshot[S, V]](startingSnapshot) {
        acc(key)
      }.drop(1)

  /**
   * Return the view of the data for the key 'key' at the specified timestamp.
   *
   * @param key The key
   * @param time The timestamp at which we want to see the view of the data
   * @return view of the data with events up to the given time stamp.
   */
  def getAt(key: K, time: DateTime)(implicit F: Monad[F], FC: Catchable[F]): F[Option[V]] = {
    import com.github.nscala_time.time.Implicits._
    // We need to get the earliest snapshot, then the stream of events from that snapshot
    for {
      earliestSnapshot <- snapshotStore.get(key, SequenceQuery.earliest[S])
      value <- snapshotFold(earliestSnapshot, eventStore.get(toStreamKey(key), earliestSnapshot.seq).takeWhile { _.time <= time }, acc(key)).map { _.value }
    } yield value
  }

  /**
   * Essentially a runFoldMap on the given process to produce a snapshot after collapsing a stream of events.
   * @param events The stream of events.
   * @return Container F that when executed provides the snapshot.
   */
  private def snapshotFold(
    start: Snapshot[S, V],
    events: Process[F, Event[KK, S, E]],
    f: (Snapshot[S, V], Event[KK, S, E]) => Snapshot[S, V]
  )(implicit F: Monad[F], FC: Catchable[F]): F[Snapshot[S, V]] =
    events.pipe {
      process1.fold(start)(f)
    }.runLastOr(start)

  /**
   * Explicitly refresh persisted snapshot with events starting at `forceStartAt`. Normally to refresh a snapshot,
   * your implementation of QueryAPI can do so asynchronously via a custom `onGenerateLatestSnapshot` function.
   *
   * WARNING - Use this only if you know that events prior to `forceStartAt` can be safely ignored. Typically this is
   * when a single event stream contains events for multiple entities, so obviously when you create a new entity, you
   * can ignore all events prior to that creation event.
   *
   * @param key The key
   * @param forceStartAt Generate a snapshot starting from events at the specified sequence number.
   *                     This should only be used when it is known that preceding events can be ignored. For example
   *                     when new entities are added, there are no views of those entities before the events that add
   *                     them!
   * @return Error when saving snapshot or the snapshot that was saved.
   */
  def forceRefreshPersistedSnapshot(key: K, forceStartAt: S)(implicit F: Monad[F], FC: Catchable[F]): F[SnapshotStorage.Error \/ Snapshot[S, V]] =
    for {
      snapshotToSave <- snapshotFold(Snapshot.zero, eventStore.get(toStreamKey(key), Some(forceStartAt)), acc(key))
      saveResult <- persistSnapshot(key, snapshotToSave, None)
    } yield saveResult

  /**
   * Save the given `snapshot` if it is at a different sequence number to `previousSnapshot`. Set `previousSnapshot`
   * to None to force a save.
   */
  def persistSnapshot(key: K, snapshot: Snapshot[S, V], previousSnapshot: Option[Snapshot[S, V]])(implicit F: Applicative[F]): F[SnapshotStorage.Error \/ Snapshot[S, V]] =
    if (snapshot.seq != previousSnapshot.map { _.seq })
      snapshotStore.put(key, snapshot, SnapshotStoreMode.Cache)
    else
      snapshot.right[SnapshotStorage.Error].point[F]
}

case class SaveAPIConfig(retry: Retry)

object SaveAPIConfig {
  val default = SaveAPIConfig(Retry.fullJitter(20, 5.millis, 2.0))
}

case class SaveAPI[F[_], KK, E, K, S, V](
    taskToF: Task ~> F,
    query: QueryAPI[F, KK, E, K, S, V]
) {
  // TODO: Maybe just make a MonadTask trait and use it as a constraint.
  private def saveWithRetry(key: K, operation: Operation[S, V, E], durations: Seq[Duration])(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]): F[SaveResult[S, V]] =
    durations match {
      case d :: ds =>
        for {
          _ <- taskToF { Task.schedule((), d) }
          old <- query.generateLatestSnapshot(key)
          op = operation.run(old.latest)
          result <- op.fold(
            EventStreamError.noop.left[Event[KK, S, E]].point[F],
            EventStreamError.reject(_).left[Event[KK, S, E]].point[F],
            e => query.eventStore.put(Event.next[KK, S, E](query.toStreamKey(key), old.latest.seq, e))
          )
          transform <- result match {
            case -\/(EventStreamError.DuplicateEvent) =>
              saveWithRetry(key, operation, ds)
            case -\/(EventStreamError.Noop) =>
              SaveResult.noop[S, V](old.latest).point[F]
            case -\/(EventStreamError.Rejected(r)) =>
              SaveResult.reject[S, V](r).point[F]
            case \/-(event) =>
              val newSnapshot = query.acc(key)(old.latest, event)

              for {
                saveResult <- SaveResult.success[S, V](newSnapshot).point[F]
                _ <- query.persistSnapshot(key, newSnapshot, old.previousPersisted.some)
              } yield saveResult
          }
        } yield transform

      case _ =>
        SaveResult.reject[S, V](NonEmptyList(Reason("Failed to save after retries"))).point[F]
    }

  def save(key: K, operation: Operation[S, V, E])(config: SaveAPIConfig)(implicit F: Monad[F], FC: Catchable[F], S: Sequence[S]): F[SaveResult[S, V]] =
    saveWithRetry(key, operation, Seq(0.milli) ++ config.retry.run)
}
