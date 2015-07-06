package io.atlassian.event
package stream
package dynamo

import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
import org.joda.time.DateTime
import org.scalacheck.Prop
import org.specs2.main.Arguments

import scalaz._
import scalaz.concurrent.Task

class DynamoSingleSnapshotStorageSpec(val arguments: Arguments) extends ScalaCheckSpec with LocalDynamoDB with DynamoDBActionMatchers {
  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  def is = stopOnFail ^ s2"""
    This is a specification to check the DynamoDB single snapshot storage

    DynamoSingleSnapshotStorage should                   ${step(startLocalDynamoDB)} ${step(createTestTable)}
       return no snapshot when there is no snapshot      ${getNoSnapshot.set(minTestsOk = NUM_TESTS)}
       get what was saved                                ${getWhatWasPut.set(minTestsOk = NUM_TESTS)}

                                                         ${step(deleteTestTable)}
                                                         ${step(stopLocalDynamoDB)}

  """
  type KK = String
  type S = Long
  type V = String

  object DynamoMappings {

    val tableName = s"DynamoSingleSnapshotStorageSpec_${System.currentTimeMillis}"
    val key = Column[KK]("key")
    val seq = Column[S]("seq")
    val value = Column[V]("value").column
    lazy val tableDefinition =
      TableDefinition.from[KK, V, KK, S](tableName, key.column, value, key, seq)

  }

  val runner: DynamoDBAction ~> Task =
    new (DynamoDBAction ~> Task) {
      def apply[A](a: DynamoDBAction[A]): Task[A] =
        a.run(DYNAMO_CLIENT).fold({ i => Task.fail(WrappedInvalidException.orUnderlying(i)) }, { a => Task.now(a) })
    }

  val DBDefinition = DynamoSingleSnapshotStorage.fromDefinition[Task, KK, S, V](DynamoMappings.tableDefinition, runner)
  val DBSnapshotStorage = DBDefinition.snapshotStore

  def getNoSnapshot = Prop.forAll { (nonEmptyKey: UniqueString) =>
    DBSnapshotStorage.get(nonEmptyKey.unwrap, SequenceQuery.latest).run.seq must beNone and
      (DBSnapshotStorage.get(nonEmptyKey.unwrap, SequenceQuery.earliest).run.seq must beNone) and
      (DBSnapshotStorage.get(nonEmptyKey.unwrap, SequenceQuery.before(1)).run.seq must beNone)
  }

  def getWhatWasPut = Prop.forAll { (nonEmptyKey: UniqueString, v: String) =>
    val expected = (Some(v), Some(1))
    (for {
      _ <- DBSnapshotStorage.put(nonEmptyKey.unwrap, Snapshot.value[S, V](v)(1, DateTime.now), SnapshotStoreMode.Cache)
      saved <- DBSnapshotStorage.get(nonEmptyKey.unwrap, SequenceQuery.latest)
    } yield (saved.value, saved.seq)).run === expected
  }

  def createTestTable() =
    DynamoDBOps.createTable(DBDefinition.tableDefinition)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(DBDefinition.tableDefinition)

}
