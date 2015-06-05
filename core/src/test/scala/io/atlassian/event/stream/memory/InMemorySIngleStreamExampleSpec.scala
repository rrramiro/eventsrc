package io.atlassian.event
package stream
package memory

import SingleStreamExample._

import scalaz.concurrent.Task

class InMemorySingleStreamExampleSpec extends SingleStreamExampleSpec {
  override protected def newEventStream(): ClientEventStream = new InMemoryClientEventStream
  override protected def newSnapshotStore() = new ClientIdClientDataSnapshotStorage

}

class InMemoryClientEventStream extends ClientEventStream(1) {
  override val eventStore = new MemoryEventStorage[SingleStreamKey, TwoPartSequence, ClientEvent]
}

class ClientIdClientDataSnapshotStorage extends MemorySingleSnapshotStorage[Task, Client.Id, TwoPartSequence, Client.Data]
