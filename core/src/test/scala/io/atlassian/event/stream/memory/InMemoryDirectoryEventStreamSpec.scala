package io.atlassian.event
package stream
package memory

import scalaz.concurrent.Task

class InMemoryDirectoryEventStreamSpec extends DirectoryEventStreamSpec {
  val getEventStore = MemoryEventStorage[DirectoryEventStream.DirectoryId, TwoPartSequence[Long], DirectoryEvent]
  val getAllUserSnapshot = MemorySingleSnapshotStorage[DirectoryEventStream.DirectoryId, TwoPartSequence[Long], List[User]]
}
