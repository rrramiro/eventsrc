package io.atlassian.event
package stream
package memory

import SingleStreamExample._

import scalaz.concurrent.Task

class InMemorySingleStreamExampleSpec extends SingleStreamExampleSpec {
  // TODO: Changing this to a val breaks things. Really not good.
  override protected def eventStore = new MemoryEventStorage[SingleStreamKey, TwoPartSequence[Long], ClientEvent]
  override protected val snapshotStore = new ClientIdClientDataSnapshotStorage
}

class ClientIdClientDataSnapshotStorage extends MemorySingleSnapshotStorage[Task, Client.Id, TwoPartSequence[Long], Client.Data]
