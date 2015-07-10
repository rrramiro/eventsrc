package io.atlassian.event
package stream

import io.atlassian.event.stream.DirectoryEventStream.DirectoryId
import org.scalacheck.Prop
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.{ \/, OptionT }
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
    val api = DirectoryEventStream.allUsersQueryAPIWithNoSnapshots(AlwaysFailingDirectoryEventStream.eventStore)
    val saveApi = DirectoryEventStream.allUsersSaveAPI(api)

    saveApi.save(k, Operation.insert(DirectoryEvent.addUser(u1)))(SaveAPIConfig.default).run.fold(
      { _ => failure },
      { _ => success },
      { _ => failure }
    )
  }.set(minTestsOk = 1)
}

object AlwaysFailingDirectoryEventStream {
  import DirectoryEventStream.DirectoryId

  val eventStore = new EventStorage[Task, DirectoryId, TwoPartSequence[Long], DirectoryEvent] {
    def get(key: DirectoryId, fromOption: Option[TwoPartSequence[Long]]) =
      Process.halt

    def put(ev: Event[DirectoryId, TwoPartSequence[Long], DirectoryEvent]) =
      Task {
        EventStreamError.duplicate.left
      }

    def latest(key: DirectoryId) = OptionT.none
  }
}
