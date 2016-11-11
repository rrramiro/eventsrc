package io.atlassian.event
package stream

import java.util.concurrent.{ Executors, ExecutorService }

import io.atlassian.event.stream.memory.{ MemoryEventStorage, MemorySingleSnapshotStorage }

import scalaz._
import scalaz.concurrent.Task
import scalaz.effect.LiftIO
import scalaz.std.option._
import scalaz.syntax.std.option._

object UserAccountExample {

  type UserId = String
  type GroupId = String
  type CompanyId = String
  case class User(id: CompanyUserId, name: String, username: String)
  case class CompanyGroupId(companyId: CompanyId, groupId: GroupId)
  case class CompanyUserId(companyId: CompanyId, userId: UserId)
  case class CompanyUsername(companyId: CompanyId, username: String)

  // Step 1. Define our events and EventStream

  sealed trait UserAccountEvent
  case class InsertUser(id: UserId, name: String,
    username: String) extends UserAccountEvent

  case class DeleteUser(id: UserId) extends UserAccountEvent

  case class SetAPIKey(id: UserId, apiKey: String)
    extends UserAccountEvent

  case class AddUserToGroup(groupId: GroupId, userId: UserId)
    extends UserAccountEvent

  case class RemoveUserFromGroup(groupId: GroupId, userId: UserId)
    extends UserAccountEvent

  import Event.syntax._

  // Step 2. Define QueryAPIs that match how we want to query/interpret our events

  def groupMembersByIdQuery(
    eventStore: EventStorage[Task, CompanyId, Long, UserAccountEvent],
    snapshotStore: SnapshotStorage[Task, CompanyGroupId, Long, List[UserId]]
  ) =
    QueryAPI[Task, CompanyId, UserAccountEvent, CompanyGroupId, Long, List[UserId]](
      _.companyId,
      eventStore,
      snapshotStore,
      (k: CompanyGroupId) => (s: Snapshot[Long, List[UserId]], e: Event[CompanyId, Long, UserAccountEvent]) =>
        e.process(s) { ov =>
          {
            case AddUserToGroup(groupId, userId) if k.groupId == groupId =>
              val currentList = s.value.getOrElse(List())
              (userId :: currentList.filterNot { _ == userId }).some
            case RemoveUserFromGroup(groupId, userId) if k.groupId == groupId =>
              val currentList = s.value.getOrElse(List())
              currentList.filterNot { _ == userId }.some
          }
        }
    )

  def userByIdQuery(
    eventStore: EventStorage[Task, CompanyId, Long, UserAccountEvent],
    snapshotStore: SnapshotStorage[Task, CompanyUserId, Long, User]
  ) =
    QueryAPI[Task, CompanyId, UserAccountEvent, CompanyUserId, Long, User](
      _.companyId,
      eventStore,
      snapshotStore,
      (k: CompanyUserId) => (s: Snapshot[Long, User], e: Event[CompanyId, Long, UserAccountEvent]) =>
        e.process(s) { ov =>
          {
            case InsertUser(id, name, username) if k.userId == id =>
              User(k, name, username).some
            case DeleteUser(id) if k.userId == id =>
              none[User]
          }
        }
    )

  def userByNameQuery(
    eventStore: EventStorage[Task, CompanyId, Long, UserAccountEvent],
    snapshotStore: SnapshotStorage[Task, CompanyUsername, Long, User]
  ) =
    QueryAPI[Task, CompanyId, UserAccountEvent, CompanyUsername, Long, User](
      _.companyId,
      eventStore,
      snapshotStore,
      (k: CompanyUsername) => (s: Snapshot[Long, User], e: Event[CompanyId, Long, UserAccountEvent]) =>
        e.process(s) { ov =>
          {
            case InsertUser(id, name, username) if k.username == username =>
              User(CompanyUserId(k.companyId, id), name, username).some
            case DeleteUser(id) =>
              ov match {
                case Some(u) => if (u.id.userId == id) none[User] else u.some
                case None    => none[User]
              }
          }
        }
    )

  // Step 3. Define SaveAPIs and a 'data layer' to save events corresponding to entities with constraints
  trait DataAccess[F[_]] {
    def saveUser(u: User): F[SaveResult[Long]]
  }

  object DataAccess {
    def apply[F[_]: Monad: LiftIO, KK](queryAPI: QueryAPI[F, KK, UserAccountEvent, CompanyUsername, Long, User]): DataAccess[F] =
      new DataAccess[F] {
        lazy val saveAPI = SaveAPI[F, KK, UserAccountEvent, CompanyUsername, Long](SaveAPI.Config.default, queryAPI.toStreamKey, queryAPI.eventStore)
        def saveUser(u: User): F[SaveResult[Long]] = {
          val event = InsertUser(u.id.userId, u.name, u.username)
          val operation = Operation[Long, UserAccountEvent] { _ =>
            Operation.Result.success(event)
          }
          saveAPI.save(CompanyUsername(u.id.companyId, u.username), operation)
        }
      }
  }

  // Step 4. Bring it all together
  {
    val saveAndGetUser: Task[Option[User]] =
      for {
        // 1. Instantiate a stream with an EventStorage
        eventStore <- MemoryEventStorage[CompanyId, Long, UserAccountEvent]

        // 2. Create QueryAPIs defined for stream
        // Just use some in-memory snapshot storage for this example
        userByIdStorage <- MemorySingleSnapshotStorage[CompanyUserId, Long, User]
        userByNameStorage <- MemorySingleSnapshotStorage[CompanyUsername, Long, User]
        userById = userByIdQuery(eventStore, userByIdStorage) // QueryAPI[Task, KK, UserAccountEvent, CompanyUsername, Long, User]
        userByName = userByNameQuery(eventStore, userByNameStorage) // QueryAPI[Task, KK, UserAccountEvent, CompanyUsername, Long, User]

        // 3. Create SaveAPIs defined for stream. This is done when creating DataAccess which has saveUser with Operation logic
        dataLayer = DataAccess[Task, CompanyId](userByName)

        _ <- dataLayer.saveUser(User(CompanyUserId("abccom", "a"), "Fred", "fred")) // Task[SaveResult[User]]
        saved <- userById.get(CompanyUserId("abccom", "a"), QueryConsistency.LatestEvent)
      } yield saved

    saveAndGetUser.run // Run it!
  }
}
