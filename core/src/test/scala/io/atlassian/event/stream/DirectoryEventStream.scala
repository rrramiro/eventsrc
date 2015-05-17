package io.atlassian.event
package stream

import argonaut._, Argonaut._
import org.scalacheck.Arbitrary._
import org.scalacheck.{ Gen, Arbitrary }

import scalaz.{ @@, \/ }
import scalaz.concurrent.Task
import scalaz.syntax.either._

sealed trait DirectoryEvent
case class AddUser(user: User) extends DirectoryEvent
object DirectoryEvent {
  def addUser(user: User): DirectoryEvent =
    AddUser(user)
}

object DirectoryEventStream {
  case class DirectoryId(id: String)
  object DirectoryId {
    implicit val DirectoryIdArbitrary: Arbitrary[DirectoryId] =
      Arbitrary { Gen.uuid.map { uuid => DirectoryId(uuid.toString) } }
  }
  type ZoneId = Long
  type Username = String
  type UserId = String
  type DirectoryUsername = (DirectoryId, Username)
  type DirectoryUsernamePrefix = (DirectoryId, String)

  object ops {
    import scalaz.syntax.std.option._
    import DataValidator._
    import Operation.syntax._

    def addUniqueUser(events: DirectoryEventStream)(u: User): Operation[events.S, List[User], events.E] =
      DirectoryEvent.addUser(u).op[events.S, List[User]].filter { noDuplicateUsername(u) }

    private def noDuplicateUsername(u: User): DataValidator.Validator[List[User]] =
      ol =>
        if ((ol | Nil).exists { _.username == u.username })
          "User with same username already exists".fail
        else
          DataValidator.success

  }
}

import DirectoryEventStream._

case class User(id: UserId, username: Username)

object User {
  implicit val UserCodecJson: CodecJson[User] =
    casecodec2(User.apply, User.unapply)("id", "username")

  implicit val ArbitraryUser: Arbitrary[User] =
    Arbitrary(
      for {
        uid <- Gen.uuid
        name <- arbitrary[String]
      } yield User(uid.toString, name)
    )
}

abstract class DirectoryEventStream(zone: ZoneId) extends EventStream[Task] {

  type KK = DirectoryId
  type S = TwoPartSequence
  type E = DirectoryEvent

  override implicit lazy val S = TwoPartSequence.twoPartSequence(zone)

  class ShardedUsernameQueryAPI extends QueryAPI[DirectoryUsername, UserId] {
    override def eventStreamKey = _._1

    override def acc(key: DirectoryUsername)(v: Snapshot[S, UserId], e: Event[KK, S, E]): Snapshot[S, UserId] =
      e.operation match {
        case AddUser(user) =>
          if (key._2 == user.username)
            Snapshot.value(user.id)(e.id.seq, e.time)
          else
            v
      }

    object snapshotStore extends SnapshotStorage[Task, DirectoryUsername, S, UserId] {
      val map = collection.concurrent.TrieMap[DirectoryUsernamePrefix, Snapshot[S, Map[Username, UserId]]]()
      def get(key: DirectoryUsername, seq: SequenceQuery[TwoPartSequence]): Task[Snapshot[S, UserId]] =
        Task {
          map.getOrElse(prefix(key), Snapshot.zero).fold(Snapshot.zero[S, UserId],
            (m, id, t) =>

              m.get(key._2).fold(Snapshot.deleted[S, UserId](id, t)) { uid => Snapshot.value(uid)(id, t) },
            (id, t) => Snapshot.deleted(id, t)) // This should not happen
        }

      def put(key: DirectoryUsername, view: Snapshot[S, UserId], mode: SnapshotStoreMode): Task[SnapshotStorage.Error \/ Snapshot[S, UserId]] =
        Task {
          map.get(prefix(key)) match {
            case None =>
              val newSnapshot: Snapshot[S, Map[Username, UserId]] =
                view.fold(Snapshot.zero[S, Map[Username, UserId]], (uid, id, t) => Snapshot.value(Map(key._2 -> uid))(id, t), Snapshot.value(Map()))
              map += (prefix(key) -> newSnapshot)
            case Some(s) =>
              (s, view) match {
                case (Snapshot.Value(m, id1, t1), Snapshot.Value(uid, id2, t2)) =>
                  map += (prefix(key) -> Snapshot.Value(m + (key._2 -> uid), id2, t2))
                case (Snapshot.Value(m, id1, t1), Snapshot.Deleted(id2, t2)) =>
                  map += (prefix(key) -> Snapshot.Value(m - key._2, id2, t2))
                case _ =>
                  ()
              }
          }
          view.right
        }

      private def prefix(key: DirectoryUsername): (DirectoryId, String) =
        (key._1, key._2.substring(0, Math.min(3, key._2.length)))
    }
  }

  class ShardedUsernameQueryAPIWithSnapshotPersistence extends ShardedUsernameQueryAPI {
    override val runPersistSnapshot: Task[SnapshotStorage.Error \/ Snapshot[S, V]] => Unit =
      _.run.fold(println(_), _ => ())
  }

  class AllUsersQueryAPI(val snapshotStore: SnapshotStorage[Task, DirectoryId, S, List[User]]) extends QueryAPI[DirectoryId, List[User]] {
    override def eventStreamKey = k => k

    override def acc(key: DirectoryId)(v: Snapshot[S, List[User]], e: Event[KK, S, E]): Snapshot[S, List[User]] =
      e.operation match {
        case AddUser(user) =>
          val userList: List[User] =
            v.value.fold(List(user)) { l => user :: l }

          Snapshot.Value(userList, e.id.seq, e.time)
      }
  }

  object AllUsersQueryAPIWithNoSnapshots extends AllUsersQueryAPI(SnapshotStorage.none)

  class AllUsersQueryAPIWithSnapshotPersistence(snapshotStore: SnapshotStorage[Task, DirectoryId, S, List[User]])
      extends AllUsersQueryAPI(snapshotStore) {
    override val runPersistSnapshot: Task[SnapshotStorage.Error \/ Snapshot[S, V]] => Unit =
      _.run.fold(e => throw new RuntimeException(e.toString), _ => ())
  }

  class AllUsersSaveAPI(query: QueryAPI[DirectoryId, List[User]]) extends SaveAPI[DirectoryId, List[User]](query)
}
