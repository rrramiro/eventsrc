package io.atlassian.event
package stream
package dynamo

import argonaut._, Argonaut._
import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
import io.atlassian.event.stream.memory.MemorySingleSnapshotStorage
import org.specs2.main.Arguments

import scalaz._
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

class DynamoDBDirectoryEventStreamSpec(val arguments: Arguments) extends DirectoryEventStreamSpec with LocalDynamoDB with DynamoDBActionMatchers {

  override def is =
    s2"""
        DirectoryEventStream supports                               ${step(startLocalDynamoDB)} ${step(createTestTable)}

          Adding multiple users (store list of users)               ${addMultipleUsers(NUM_TESTS)}

                                                                    ${step(deleteTestTable)}
                                                                    ${step(stopLocalDynamoDB)}
      """
  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  val runner: DynamoDBAction ~> Task =
    new (DynamoDBAction ~> Task) {
      def apply[A](a: DynamoDBAction[A]): Task[A] =
        a.run(DYNAMO_CLIENT).fold({ i => Task.fail(WrappedInvalidException.orUnderlying(i)) }, { a => Task.now(a) })
    }

  def createTestTable() =
    DynamoDBOps.createTable(DirectoryEventStreamDynamoMappings.schema)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(DirectoryEventStreamDynamoMappings.schema)

  val getEventStore = DynamoDirectoryEventStream.eventStore(runner).point[Task]
  val getAllUserSnapshot = MemorySingleSnapshotStorage[DirectoryEventStream.DirectoryId, TwoPartSequence[Long], List[User]]
}

import DirectoryEventStream._

object DirectoryEventStreamDynamoMappings {
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

  val key = Column[ColumnDirectoryId]("key")
  val seq = Column[ColumnTwoPartSequence]("seq")
  val event = Column[DirectoryEvent]("event").column
  val tableName = s"dynamo_dir_event_stream_test_${System.currentTimeMillis}"
  val schema = TableDefinition.from[ColumnDirectoryId, DirectoryEvent, ColumnDirectoryId, ColumnTwoPartSequence](tableName, key.column, event, key, seq)
}

object DynamoDirectoryEventStream {
  import DirectoryEventStreamDynamoMappings.schema

  def eventStore(runner: DynamoDBAction ~> Task) =
    new DynamoEventStorage[Task, ColumnDirectoryId, ColumnTwoPartSequence, DirectoryEvent](
      schema,
      runner,
      NaturalTransformation.refl
    ).mapKS(
      ColumnDirectoryId.iso.from,
      ColumnDirectoryId.iso.to,
      ColumnTwoPartSequence.iso.from,
      ColumnTwoPartSequence.iso.to
    )
}
