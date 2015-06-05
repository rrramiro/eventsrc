package io.atlassian.event
package stream
package dynamo

import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
import io.atlassian.event.stream.memory.MemorySingleSnapshotStorage
import kadai.Attempt
import org.specs2.main.Arguments
import scodec.bits.ByteVector

import scalaz._
import scalaz.concurrent.Task

class DynamoSingleStreamExampleSpec(val arguments: Arguments) extends SingleStreamExampleSpec with LocalDynamoDB with DynamoDBActionMatchers {
  override def is =
    s2"""
        Dynamo implementation of single event stream supports       ${step(startLocalDynamoDB)} ${step(createTestTable)}
          Add and get                                               ${addAndGetClientById.set(minTestsOk = NUM_TESTS)}
          Add and delete                                            ${addAndDelete.set(minTestsOk = NUM_TESTS)}

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
    DynamoDBOps.createTable(ClientEventStreamDynamoMappings.schema)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(ClientEventStreamDynamoMappings.schema)

  override protected def newEventStream() = new DynamoClientEventStream(1)(runner)
  override protected def newSnapshotStore() = new ClientIdClientDataSnapshotStorage

}

import SingleStreamExample._

object ClientEventStreamDynamoMappings {

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

  implicit val SingleStreamKeyEncoder: Encoder[SingleStreamKey] =
    Encoder[String].contramap { _.unwrap }
  implicit val SingleStreamKeyDecoder: Decoder[SingleStreamKey] =
    Decoder[String].map { SingleStreamKey.apply }

  val key = Column[SingleStreamKey]("key")
  val seq = Column[TwoPartSequence]("seq")
  val event = Column[ClientEvent]("event")
  val tableName = s"dynamo_client_event_stream_test_${System.currentTimeMillis}"
  val schema = TableDefinition.from[SingleStreamKey, ClientEvent, SingleStreamKey, TwoPartSequence](tableName, key, event, key, seq)

}

class DynamoClientEventStream(zone: ZoneId)(runner: DynamoDBAction ~> Task) extends ClientEventStream(zone) {
  import ClientEventStreamDynamoMappings._

  val TaskToTask = NaturalTransformation.refl[Task]

  val eventStore = new DynamoEventStorage[Task, KK, S, E](schema, runner, TaskToTask)
}

class ClientIdClientDataSnapshotStorage extends MemorySingleSnapshotStorage[Task, Client.Id, TwoPartSequence, Client.Data]