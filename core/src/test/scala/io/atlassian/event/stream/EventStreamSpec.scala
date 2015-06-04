package io.atlassian.event
package stream

import io.atlassian.event.stream.DirectoryEventStream.DirectoryId
import org.scalacheck.Prop
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.\/
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._

class EventStreamSpec extends SpecificationWithJUnit with ScalaCheck {

  def is =
    s2"""

         This specification tests some parts of EventStream

         EventStream.SaveAPI retries                          $saveAPIRetries
    """

  def saveAPIRetries = Prop.forAll { (k: DirectoryId, u1: User) =>
    val es = new AlwaysFailingDirectoryEventStream
    val api = es.AllUsersQueryAPIWithNoSnapshots
    val saveApi = new es.AllUsersSaveAPI(api)

    saveApi.save(k, Operation.insert(DirectoryEvent.addUser(u1))).run.fold(
      { _ => failure },
      { _ => success },
      { _ => failure }
    )
  }

  class AlwaysFailingDirectoryEventStream extends DirectoryEventStream(1) {
    override val eventStore = new EventStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence, DirectoryEvent] {
      override def get(key: KK, fromOption: Option[S]): Process[Task, Event[KK, S, E]] =
        Process.halt

      override def put(ev: Event[KK, S, E]): Task[EventStream.Error \/ Event[KK, S, E]] =
        Task {
          EventStream.Error.duplicate.left
        }
    }
  }

}
