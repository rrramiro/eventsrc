package io.atlassian.event
package stream

import java.util.concurrent.ExecutorService

import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

import scalaz.{ \/, @@ }
import Event.syntax._
import scalaz.concurrent.Task
import scalaz.std.option._
import scalaz.syntax.std.option._
import argonaut._, Argonaut._

object SingleStreamExample {
  type ZoneId = Long
  type SingleStreamKey = String @@ SingleStreamKey.Marker
  object SingleStreamKey extends Tagger[String] {
    val VAL = apply("1")
  }

  sealed trait ClientEvent
  case class Insert(k: Client.Id, v: Client.Data) extends ClientEvent
  case class Delete(k: Client.Id) extends ClientEvent
  object ClientEvent {
    def insert(k: Client.Id, v: Client.Data): ClientEvent =
      Insert(k, v)
    def delete(k: Client.Id): ClientEvent =
      Delete(k)
  }

  implicit val ClientIdEncodeJson: EncodeJson[Client.Id] =
    implicitly[EncodeJson[String]].contramap { _.unwrap }

  implicit val ClientIdDecodeJson: DecodeJson[Client.Id] =
    implicitly[DecodeJson[String]].map { Client.Id.apply }

  implicit val ClientDataCodecJson: CodecJson[Client.Data] =
    casecodec1(Client.Data.apply, Client.Data.unapply)("name")

  private implicit val ClientInsertCodecJson: CodecJson[Insert] =
    casecodec2(Insert.apply, Insert.unapply)("id", "data")

  private implicit val ClientDeleteCodecJson: CodecJson[Delete] =
    casecodec1(Delete.apply, Delete.unapply)("id")

  implicit val ClientEventEncodeJson: EncodeJson[ClientEvent] =
    EncodeJson {
      case e @ Insert(_, _) => ("insert" := e) ->: jEmptyObject
      case e @ Delete(_) => ("delete" := e) ->: jEmptyObject
    }

  implicit val ClientEventDecodeJson: DecodeJson[ClientEvent] =
    DecodeJson { c =>
      (c --\ "insert").as[Insert].map[ClientEvent] { identity } |||
        (c --\ "delete").as[Delete].map[ClientEvent] { identity } |||
        DecodeResult.fail("Invalid client event", c.history)
    }

  object Client {
    type Id = String @@ Id.Marker
    object Id extends Tagger[String]

    case class Data(name: String)
  }
  implicit lazy val ArbitraryClientData: Arbitrary[Client.Data] =
    Arbitrary {
      for {
        name <- arbitrary[String]
      } yield Client.Data(name)
    }
  implicit lazy val ArbitraryClientId: Arbitrary[Client.Id] =
    Arbitrary {
      Gen.uuid.map {
        _.toString
      }.map {
        Client.Id.apply
      }
    }

  def clientEventStream[K](
    eventStore: EventStorage[Task, SingleStreamKey, K, ClientEvent],
    snapshotStore: SnapshotStorage[Task, Client.Id, K, Client.Data]
  ) =
    QueryAPI[Task, SingleStreamKey, ClientEvent, Client.Id, K, Client.Data](
      _ => SingleStreamKey.VAL,
      eventStore,
      snapshotStore,
      (key: Client.Id) => (s: Snapshot[K, Client.Data], e: Event[SingleStreamKey, K, ClientEvent]) => e.process(s) { ov =>
        {
          case Insert(k, v) if key == k =>
            v.some
          case Delete(k) =>
            none
        }
      }
    )
}
