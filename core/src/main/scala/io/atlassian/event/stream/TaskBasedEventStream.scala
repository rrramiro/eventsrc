package io.atlassian.event.stream

import java.util.concurrent.ExecutorService

import kadai.Invalid

import scalaz.concurrent.Task
import scalaz.{ \/, -\/, \/- }
import scalaz.syntax.either._

/**
 * Slightly concrete-ized implementation of `EventStream` using `Task` with basic API implementations for asynchronously
 * refreshing any persisted snapshots.
 */
abstract class TaskBasedEventStream extends EventStream[Task] {
  /**
   * Version of QueryAPI that persists latest snapshot after generation.
   */
  trait AsyncRefreshingQueryAPI[Key, Val] extends QueryAPI[Key, Val] {
    /**
     * Used to run asynchronous operations
     */
    def executorService: ExecutorService

    override val runPersistSnapshot: Task[SnapshotStorage.Error \/ Snapshot[S, V]] => Unit =
      a => Task.fork(a)(executorService).runAsync {
        case -\/(e) => handlePersistLatestSnapshot(SnapshotStorage.Error.unknown(Invalid.Err(e)).left)
        case \/-(r) => handlePersistLatestSnapshot(r)
      }

    protected def handlePersistLatestSnapshot: SnapshotStorage.Error \/ Snapshot[S, V] => Unit
  }
}
