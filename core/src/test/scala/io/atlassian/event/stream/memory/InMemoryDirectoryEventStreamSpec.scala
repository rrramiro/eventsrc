package io.atlassian.event
package stream
package memory

import scalaz.concurrent.Task

class InMemoryDirectoryEventStreamSpec extends DirectoryEventStreamSpec {
  override protected def newEventStream(): DirectoryEventStream = new InMemoryDirectoryEventStream
  override protected def allUserSnapshot() = DirectoryIdListUserSnapshotStorage
}

class InMemoryDirectoryEventStream extends DirectoryEventStream(1) {
  override val eventStore = new MemoryEventStorage[DirectoryEventStream.DirectoryId, TwoPartSequence, DirectoryEvent]
}

object DirectoryIdListUserSnapshotStorage extends MemorySingleSnapshotStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence, List[User]]