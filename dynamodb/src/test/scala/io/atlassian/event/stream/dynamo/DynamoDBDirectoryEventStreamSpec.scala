package io.atlassian.event
package stream
package dynamo

import argonaut._, Argonaut._
import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
import io.atlassian.event.stream.memory.MemorySingleSnapshotStorage
import kadai.Attempt
import org.specs2.main.Arguments
import scodec.bits.ByteVector

import scalaz._
import scalaz.concurrent.Task

class DynamoDBDirectoryEventStreamSpec(val arguments: Arguments) extends DirectoryEventStreamSpec with LocalDynamoDB with DynamoDBActionMatchers {

  override def is =
    s2"""
        DirectoryEventStream supports                               ${step(startLocalDynamoDB)} ${step(createTestTable)}
          Adding multiple users (store list of users)               ${addMultipleUsers.set(minTestsOk = NUM_TESTS)}
          Checking for duplicate usernames (store list of users)    ${duplicateUsername.set(minTestsOk = NUM_TESTS)}
          Adding multiple users (store list of users and snapshot)  ${addMultipleUsersWithSnapshot.set(minTestsOk = NUM_TESTS)}
          Checking for duplicate usernames (store list of users and snapshot) ${duplicateUsernameWithSnapshot.set(minTestsOk = NUM_TESTS)}

          Checking for duplicate usernames with sharded store       ${duplicateUsernameSharded.set(minTestsOk = NUM_TESTS)}
          Checking for duplicate usernames with sharded store and snapshot ${duplicateUsernameShardedWithSnapshot.set(minTestsOk = NUM_TESTS)}

                                                                    ${step(deleteTestTable)}
                                                                    ${step(stopLocalDynamoDB)}
      """
  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  lazy val runner: DynamoDBAction ~> Task =
    new (DynamoDBAction ~> Task) {
      def apply[A](a: DynamoDBAction[A]): Task[A] =
        a.run(DYNAMO_CLIENT).fold({ i => Task.fail(WrappedInvalidException.orUnderlying(i)) }, { a => Task.now(a) })
    }

  def createTestTable() =
    DynamoDBOps.createTable(DirectoryEventStreamDynamoMappings.schema)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(DirectoryEventStreamDynamoMappings.schema)

  override protected def newEventStream() = new DynamoDirectoryEventStream(1)(runner)
  override protected def allUserSnapshot() = DirectoryIdListUserSnapshotStorage

}

import DirectoryEventStream._

object DirectoryEventStreamDynamoMappings {
  implicit val TwoPartSeqEncoder: Encoder[TwoPartSequence] =
    Encoder[NonEmptyBytes].contramap { seq =>
      ByteVector.fromLong(seq.seq) ++ ByteVector.fromLong(seq.zone) match {
        case NonEmptyBytes(b) => b
      }
    }

  implicit val TwoPartSeqDecoder: Decoder[TwoPartSequence] =
    Decoder[NonEmptyBytes].mapAttempt { bytes =>
      if (bytes.bytes.length != 16)
        Attempt.fail(s"Invalid length of byte vector ${bytes.bytes.length}")
      else
        Attempt.safe {
          bytes.bytes.splitAt(8) match {
            case (abytes, bbytes) => TwoPartSequence(abytes.toLong(), bbytes.toLong())
          }
        }
    }

  implicit val DirectoryIdEncoder: Encoder[DirectoryId] =
    Encoder[String].contramap { _.id }

  implicit val DirectoryIdDecoder: Decoder[DirectoryId] =
    Decoder[String].map { DirectoryId.apply }

  implicit val DirectoryEventEncodeJson: EncodeJson[DirectoryEvent] =
    EncodeJson {
      case a @ AddUser(u) => ("add-user" := u) ->: jEmptyObject
    }

  private[dynamo] val AddUserDecodeJson: DecodeJson[AddUser] =
    DecodeJson { c =>
      for {
        u <- c.get[User]("add-user")
      } yield AddUser(u)
    }

  implicit val DirectoryEventDecodeJson: DecodeJson[DirectoryEvent] =
    AddUserDecodeJson map identity

  val key = Column[DirectoryId]("key")
  val seq = Column[TwoPartSequence]("seq")
  val event = Column[DirectoryEvent]("event")
  val tableName = s"dynamo_dir_event_stream_test_${System.currentTimeMillis}"
  val schema = TableDefinition.from[DirectoryId, DirectoryEvent, DirectoryId, TwoPartSequence](tableName, key, event, key, seq)
}

class DynamoDirectoryEventStream(zone: ZoneId)(runner: DynamoDBAction ~> Task) extends DirectoryEventStream(zone) {
  import DirectoryEventStreamDynamoMappings._

  val TaskToTask = NaturalTransformation.refl[Task]

  val eventStore = new DynamoEventStorage[Task, KK, S, E](schema, runner, TaskToTask)
}

object DirectoryIdListUserSnapshotStorage extends MemorySingleSnapshotStorage[Task, DirectoryEventStream.DirectoryId, TwoPartSequence, List[User]]