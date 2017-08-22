package io.atlassian.event
package stream
package memory

import SingleStreamExample._

import scalaz.concurrent.Task

class InMemorySingleStreamExampleSpec extends SingleStreamExampleSpec {
  val getEventStore = MemoryEventStorage[SingleStreamKey, TwoPartSequence[Long], ClientEvent]()
  val getSnapshotStore = MemorySingleSnapshotStorage[Client.Id, TwoPartSequence[Long], Client.Data]
}
