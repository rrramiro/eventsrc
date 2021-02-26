package io.atlassian.event
package stream

import io.atlassian.event.stream.DirectoryEventStream.DirectoryId
import org.scalacheck.Prop
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.{ OptionT, Traverse, \/ }
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.traverse._

class EventStreamSpec extends SpecificationWithJUnit with ScalaCheck {

  def is =
    s2"""

         This specification tests some parts of EventStream

         EventStream.SaveAPI retries                          $saveAPIRetries
    """

  def saveAPIRetries = Prop.forAll { (k: DirectoryId, u1: User) =>
    val api = DirectoryEventStream.allUsersQueryAPIWithNoSnapshots(AlwaysFailingDirectoryEventStream.eventStore)
    val saveApi = DirectoryEventStream.allUsersSaveAPI(AlwaysFailingDirectoryEventStream.eventStore)

    saveApi.save(k, Operation.insert(DirectoryEvent.addUser(u1))).unsafePerformSync.fold(
      { _ => failure }, // Fail if we somehow succeeded
      { _ => failure },
      { success })
  }.set(minTestsOk = 1)
}

object AlwaysFailingDirectoryEventStream {
  import DirectoryEventStream.DirectoryId

  type DirEvent = Event[DirectoryId, TwoPartSequence[Long], DirectoryEvent]

  val eventStore =
    new EventStorage[Task, DirectoryId, TwoPartSequence[Long], DirectoryEvent] {
      override def get(key: DirectoryId, fromOption: Option[TwoPartSequence[Long]]) =
        Process.halt

      override def put(ev: DirEvent): Task[EventStreamError \/ DirEvent] =
        Task {
          EventStreamError.duplicate.left
        }

      override def latest(key: DirectoryId) =
        OptionT.none
    }
}
