package io.atlassian.event
package stream
package unsafe
package dynamo

import io.atlassian.aws.dynamodb.DynamoDB.ReadConsistency
import io.atlassian.aws.dynamodb._
import io.atlassian.event.source.Transform
import io.atlassian.event.stream.EventStreamError.Rejected
import io.atlassian.event.stream.dynamo.DynamoEventStorage
import org.joda.time.{ DateTime, DateTimeZone }
import org.junit.runner.RunWith
import org.scalacheck.Prop
import org.specs2.main.Arguments

import scalaz._
import scalaz.concurrent.Task
import scalaz.std.anyVal._
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.std.list._

@RunWith(classOf[org.specs2.runner.JUnitRunner])
class UnsafeRewritableDynamoEventStorageSpec(val arguments: Arguments) extends ScalaCheckSpec with LocalDynamoDB with DynamoDBActionMatchers {

  type KK = String
  type S = Long
  type V = String
  type E = Transform[V]

  object DynamoMappings {
    val tableName = s"DynamoEventStorageSpec_${System.currentTimeMillis}"
    val key = Column[KK]("key")
    val seq = Column[S]("seq")
    val value = Column[V]("value").column
    implicit val transformOpDecoder: Decoder[Transform.Op] =
      Decoder[String].collect(Function.unlift(Transform.Op.unapply))
    implicit val transformOpEncoder: Encoder[Transform.Op] =
      Encoder[String].contramap(Transform.Op.apply)
    val transform = Column.compose2[E](Column[Transform.Op]("Operation").column, value.liftOption) {
      case Transform.Delete    => (Transform.Op.Delete, None)
      case Transform.Insert(v) => (Transform.Op.Insert, Some(v))
    } {
      case (Transform.Op.Insert, Some(v)) => Transform.Insert(v)
      case (Transform.Op.Insert, None)    => ??? // shouldn't happen
      case (Transform.Op.Delete, None)    => ??? // We shouldn't ever be deleting things
      case (Transform.Op.Delete, Some(v)) => ??? // This shouldn't happen because the delete shouldn't have any data
    }

    lazy val tableDefinition =
      TableDefinition.from[KK, E, KK, S](tableName, key.column, transform, key, seq)

  }

  lazy val runner = TaskTransformation.runner(DYNAMO_CLIENT)

  object UnsafeRewritableDBEventStorage$ extends UnsafeRewritableDynamoEventStorage[Task, KK, S, E](DynamoMappings.tableDefinition, runner, ReadConsistency.Eventual)
  object DBEventStorage extends DynamoEventStorage[Task, KK, S, E](DynamoMappings.tableDefinition, runner, ReadConsistency.Eventual)

  implicit val JodaDateTimeEqual: Equal[DateTime] =
    Equal.equalBy { _.withZone(DateTimeZone.UTC).toInstant.getMillis }

  implicit def EventEqual[A, B, C]: Equal[Event[A, B, C]] = Equal.equal { (a, b) =>
    a.id == b.id && implicitly[Equal[DateTime]].equal(a.time, b.time) &&
      a.operation == b.operation
  }

  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  def is = stopOnFail ^ s2"""
    This is a specification to check the unsafe RewritableDynamoEventStorage

    DynamoEventStorage should                            ${step(startLocalDynamoDB)} ${step(createTestTable)}
       return an eror when events have changed           ${eventReturnsErrorForEventChanged.set(minTestsOk = NUM_TESTS)}

                                                         ${step(deleteTestTable)}
                                                         ${step(stopLocalDynamoDB)}
  """

  def eventReturnsErrorForEventChanged =
    Prop.forAll { (nonEmptyKey: UniqueString, oldValue: String, newValue: String) =>
      val eventId = EventId.first(nonEmptyKey.unwrap)
      val seq = DateTime.now
      val oldEvent = Event(eventId, seq, Transform.insert(oldValue))
      val oldEventChanged = Event(eventId, seq, Transform.insert(oldValue+"blah"))
      val newEvent = Event(eventId, seq, Transform.insert(newValue))
      (for {
        _ <- DBEventStorage.put(oldEventChanged)
        result <- UnsafeRewritableDBEventStorage$.unsafeRewrite(oldEvent, newEvent)
      } yield result).unsafePerformSyncAttempt match {
        case \/-(-\/(e)) => e === Rejected
        case _           => ko
      }
    }

  def createTestTable() =
    DynamoDBOps.createTable(DynamoMappings.tableDefinition)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(DynamoMappings.tableDefinition)
}
