package io.atlassian.event
package stream
package dynamo

import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb.DynamoDB.ReadConsistency
import io.atlassian.aws.dynamodb._
import io.atlassian.event.stream.memory.MemorySingleSnapshotStorage
import org.specs2.main.Arguments

import scalaz._
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

class DynamoSingleStreamExampleSpec(val arguments: Arguments) extends SingleStreamExampleSpec with LocalDynamoDB with DynamoDBActionMatchers {
  override def is =
    s2"""
        Dynamo implementation of single event stream supports       ${step(startLocalDynamoDB)} ${step(createTestTable)}
          Add and get                                               ${addAndGetClientById(NUM_TESTS)}
          Add and delete                                            ${addAndDelete(NUM_TESTS)}

                                                                    ${step(deleteTestTable)}
                                                                    ${step(stopLocalDynamoDB)}
      """
  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  lazy val runner = TaskTransformation.runner(DYNAMO_CLIENT)

  def createTestTable() =
    DynamoDBOps.createTable(ClientEventStreamDynamoMappings.schema)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(ClientEventStreamDynamoMappings.schema)

  val getEventStore = DynamoClientEventStream.eventStore(runner).point[Task]

  import SingleStreamExample.Client
  val getSnapshotStore = MemorySingleSnapshotStorage[Client.Id, TwoPartSequence[Long], Client.Data]

}

import SingleStreamExample._

object ClientEventStreamDynamoMappings {
  val key = Column[ColumnSingleStreamKey]("key")

  val seq = Column[ColumnTwoPartSequence]("seq")

  val event = Column[ClientEvent]("event").column
  val tableName = s"dynamo_client_event_stream_test_${System.currentTimeMillis}"
  val schema = TableDefinition.from[ColumnSingleStreamKey, ClientEvent, ColumnSingleStreamKey, ColumnTwoPartSequence](tableName, key.column, event, key, seq)

}

object DynamoClientEventStream {
  import ClientEventStreamDynamoMappings.schema

  def eventStore(runner: DynamoDBAction ~> Task) =
    new DynamoEventStorage(
      schema,
      runner,
      ReadConsistency.Eventual
    ).mapKS(
      ColumnSingleStreamKey.iso.from,
      ColumnSingleStreamKey.iso.to,
      ColumnTwoPartSequence.iso.from,
      ColumnTwoPartSequence.iso.to
    )
}
