package io.atlassian.event
package stream

import argonaut._, Argonaut._
import org.scalacheck.Arbitrary._
import org.scalacheck.{ Gen, Arbitrary }

import scalaz.{ \/, NaturalTransformation }
import scalaz.concurrent.Task
import scalaz.syntax.either._
import scalaz.syntax.std.option._
import Event.syntax._

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
    import Operation.syntax._

    def addUser(u: User): Operation[TwoPartSequence[Long], DirectoryEvent] =
      DirectoryEvent.addUser(u).op[TwoPartSequence[Long]]
  }

  val shardedUsernameSnapshotStore = new SnapshotStorage[Task, DirectoryUsername, TwoPartSequence[Long], UserId] {
    val map = collection.concurrent.TrieMap[DirectoryUsernamePrefix, Snapshot[TwoPartSequence[Long], Map[Username, UserId]]]()
    def get(key: DirectoryUsername, seq: SequenceQuery[TwoPartSequence[Long]]): Task[Snapshot[TwoPartSequence[Long], UserId]] =
      Task {
        map.getOrElse(prefix(key), Snapshot.zero)
          .fold(
            Snapshot.zero[TwoPartSequence[Long], UserId],
            (m, id, t) =>
              m.get(key._2).fold(
                Snapshot.deleted[TwoPartSequence[Long], UserId](id, t)
              ) { uid =>
                  Snapshot.value(uid)(id, t)
                },
            (id, t) => Snapshot.deleted(id, t)
          ) // This should not happen
      }

    def put(key: DirectoryUsername, view: Snapshot[TwoPartSequence[Long], UserId], mode: SnapshotStoreMode): Task[SnapshotStorage.Error \/ Snapshot[TwoPartSequence[Long], UserId]] =
      Task {
        map.get(prefix(key)) match {
          case None =>
            val newSnapshot: Snapshot[TwoPartSequence[Long], Map[Username, UserId]] =
              view.fold(Snapshot.zero[TwoPartSequence[Long], Map[Username, UserId]], (uid, id, t) => Snapshot.value(Map(key._2 -> uid))(id, t), Snapshot.value(Map()))
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

  // TODO: Partiality!?
  def partialShardedQuery[S, E] =
    (key: DirectoryUsername) => (v: Snapshot[S, UserId], e: Event[DirectoryId, S, E]) => e.process(v) { ov =>
      {
        case AddUser(user) if key._2 == user.username => user.id.some
      }
    }

  def shardedUsernameQueryAPI[E](eventStore: EventStorage[Task, DirectoryId, TwoPartSequence[Long], E]) =
    QueryAPI[Task, DirectoryId, E, DirectoryUsername, TwoPartSequence[Long], UserId](
      _._1,
      eventStore,
      shardedUsernameSnapshotStore,
      partialShardedQuery
    )

  def partialAllQuery[E, S] =
    (key: DirectoryId) => (v: Snapshot[S, List[User]], e: Event[DirectoryId, S, E]) => e.process(v) { ov =>
      {
        case AddUser(user) =>
          ov.fold(List(user)) { l => user :: l }.some
      }
    }

  def allUsersQueryAPI[E, S](
    eventStore: EventStorage[Task, DirectoryId, S, E],
    snapshotStore: SnapshotStorage[Task, DirectoryId, S, List[User]]
  )(implicit S: Sequence[S]): QueryAPI[Task, DirectoryId, S, E, List[User]] =
    QueryAPI[Task, DirectoryId, E, DirectoryId, S, List[User]](
      identity,
      eventStore,
      snapshotStore,
      partialAllQuery
    )

  def allUsersQueryAPIWithNoSnapshots[E, S](
    eventStore: EventStorage[Task, DirectoryId, S, E]
  )(implicit S: Sequence[S]): QueryAPI[Task, DirectoryId, S, E, List[User]] =
    allUsersQueryAPI(eventStore, SnapshotStorage.none)

  def allUsersSaveAPI[K, S: Sequence, E](eventStore: EventStorage[Task, K, S, E]): SaveAPI[Task, K, S, E] =
    SaveAPI[Task, K, S, E](eventStore)
}

import DirectoryEventStream.{ UserId, Username }

case class User(id: UserId, username: Username)

object User {
  implicit val UserCodecJson: CodecJson[User] =
    casecodec2(User.apply, User.unapply)("id", "username")

  implicit val ArbitraryUser: Arbitrary[User] =
    Arbitrary(
      for {
        uid <- Gen.uuid
        name <- arbitrary[Username]
      } yield User(uid.toString, name)
    )
}
