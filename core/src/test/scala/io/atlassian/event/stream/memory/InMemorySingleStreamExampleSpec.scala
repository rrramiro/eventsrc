package io.atlassian.event
package stream
package memory

import SingleStreamExample._

import scalaz.concurrent.Task

class InMemorySingleStreamExampleSpec extends SingleStreamExampleSpec {
  override protected val eventStore = new MemoryEventStorage[SingleStreamKey, TwoPartSequence[Long], ClientEvent]
  override protected val snapshotStore = new ClientIdClientDataSnapshotStorage
}

class ClientIdClientDataSnapshotStorage extends MemorySingleSnapshotStorage[Task, Client.Id, TwoPartSequence[Long], Client.Data]
