package io.atlassian.event
package stream
package memory

import scalaz.concurrent.Task

class InMemoryDirectoryEventStreamSpec extends DirectoryEventStreamSpec {
  override protected val eventStore = new MemoryEventStorage[DirectoryEventStream.DirectoryId, TwoPartSequence[Long], DirectoryEvent]
  override protected def allUserSnapshot() = DirectoryIdListUserSnapshotStorage
}

object DirectoryIdListUserSnapshotStorage extends MemorySingleSnapshotStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence[Long], List[User]]
