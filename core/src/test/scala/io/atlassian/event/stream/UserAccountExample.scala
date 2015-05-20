package io.atlassian.event
package stream

import java.util.concurrent.{ Executors, ExecutorService }

import io.atlassian.event.stream.memory.{ MemoryEventStorage, MemorySingleSnapshotStorage }

import scalaz.concurrent.Task
import scalaz.{ NonEmptyList, \/ }
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

  class UserAccountEventStream(val eventStore: EventStorage[Task, CompanyId, Long, UserAccountEvent])
      extends TaskBasedEventStream {
    type E = UserAccountEvent
    type S = Long
    type KK = CompanyId
    override implicit lazy val S = Sequence[Long]

    type Ev = Event[KK, S, E]

    import Event.syntax._

    // Step 2. Define QueryAPIs that match how we want to query/interpret our events
    class GroupMembersById(val snapshotStore: SnapshotStorage[Task, CompanyGroupId, S, List[UserId]],
        val executorService: ExecutorService) extends AsyncRefreshingQueryAPI[CompanyGroupId, List[UserId]] {

      def toStreamKey: CompanyGroupId => CompanyId = _.companyId

      def acc(k: CompanyGroupId)(s: Snapshot[Long, List[UserId]], e: Ev) =
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

      protected def handlePersistLatestSnapshot: SnapshotStorage.Error \/ Snapshot[S, V] => Unit =
        _.fold(e => println(e), _ => ())
    }

    class UserById(val snapshotStore: SnapshotStorage[Task, CompanyUserId, S, User],
        val executorService: ExecutorService) extends AsyncRefreshingQueryAPI[CompanyUserId, User] {

      def toStreamKey: CompanyUserId => CompanyId = _.companyId

      def acc(k: CompanyUserId)(s: Snapshot[Long, User], e: Ev) =
        e.process(s) { ov =>
          {
            case InsertUser(id, name, username) if k.userId == id =>
              User(k, name, username).some
            case DeleteUser(id) if k.userId == id =>
              none[User]
          }
        }

      protected def handlePersistLatestSnapshot: SnapshotStorage.Error \/ Snapshot[S, V] => Unit =
        _.fold(e => println(e), _ => ())
    }

    class UserByName(val snapshotStore: SnapshotStorage[Task, CompanyUsername, S, User],
        val executorService: ExecutorService) extends AsyncRefreshingQueryAPI[CompanyUsername, User] {

      def toStreamKey: CompanyUsername => CompanyId = _.companyId

      def acc(k: CompanyUsername)(s: Snapshot[Long, User], e: Ev) =
        e.process(s) { ov =>
          {
            case InsertUser(id, name, username) if k.username == username =>
              User(CompanyUserId(k.companyId, id), name, username).some
            case DeleteUser(id) =>
              ov match {
                case Some(u) => if (u.id.userId == id) none[User] else u.some
                case None => none[User]
              }
          }
        }

      protected def handlePersistLatestSnapshot: SnapshotStorage.Error \/ Snapshot[S, V] => Unit =
        _.fold(e => println(e), _ => ())
    }

  }

  // Step 3. Define SaveAPIs and a 'data layer' to save events corresponding to entities with constraints
  trait DataAccess {
    def saveUser(u: User): Task[SaveResult[Long, User]]
  }

  object DataAccess {
    // Because path dependent types work only for functions...
    def apply(e: UserAccountEventStream)(queryAPI: e.QueryAPI[CompanyUsername, User]): DataAccess =
      new DataAccess {
        lazy val saveAPI = new e.SaveAPI[CompanyUsername, User](queryAPI)
        def saveUser(u: User): Task[SaveResult[Long, User]] = {
          val event = InsertUser(u.id.userId, u.name, u.username)
          val operation = Operation[Long, User, e.E] {
            _.value match {
              case None =>
                Operation.Result.success(event)
              case Some(x) if u.id == x.id =>
                Operation.Result.success(event)
              case _ =>
                Operation.Result.reject(NonEmptyList(Reason("Duplicate username")))
            }
          }
          saveAPI.save(CompanyUsername(u.id.companyId, u.username), operation)
        }
      }
  }

  // Step 4. Bring it all together
  {
    // 1. Instantiate a stream with an EventStorage
    val eventStore = new MemoryEventStorage[CompanyId, Long, UserAccountEvent]
    val stream = new UserAccountEventStream(eventStore)

    // 2. Create QueryAPIs defined for stream
    // Just use some in-memory snapshot storage for this example
    object UserByIdStorage extends MemorySingleSnapshotStorage[Task, CompanyUserId, Long, User]
    object UserByNameStorage extends MemorySingleSnapshotStorage[Task, CompanyUsername, Long, User]
    val backgroundSnapshotRefresh = Executors.newCachedThreadPool()
    val userById = new stream.UserById(UserByIdStorage, backgroundSnapshotRefresh) // QueryAPI[CompanyUserId, User]
    val userByName = new stream.UserByName(UserByNameStorage, backgroundSnapshotRefresh) // QueryAPI[CompanyUsername, User]

    // 3. Create SaveAPIs defined for stream. This is done when creating DataAccess which has saveUser with Operation logic
    val dataLayer = DataAccess(stream)(userByName)

    val saveAndGetUser: Task[Option[User]] =
      for {
        _ <- dataLayer.saveUser(User(CompanyUserId("abccom", "a"), "Fred", "fred")) // Task[SaveResult[User]]
        saved <- userById.get(CompanyUserId("abccom", "a"))
      } yield saved

    saveAndGetUser.run // Run it!
  }

}
