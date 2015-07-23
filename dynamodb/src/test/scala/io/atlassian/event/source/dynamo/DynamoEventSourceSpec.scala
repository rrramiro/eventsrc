package io.atlassian.event
package source
package dynamo

import org.junit.runner.RunWith
import org.specs2.main.Arguments
import io.atlassian.aws.dynamodb._
import scalaz.std.anyVal._
import scalaz.concurrent.Task
import org.scalacheck.Prop
import org.joda.time.{ DateTimeZone, DateTime }
import scalaz._
import EventSource.Error.DuplicateEvent

@RunWith(classOf[org.specs2.runner.JUnitRunner])
class DynamoEventSourceSpec(val arguments: Arguments) extends ScalaCheckSpec with LocalDynamoDB with DynamoDBActionMatchers {

  object DBEventSource extends DynamoEventSource[String, String, Long] with LongSequencedEventSource[String, String] {
    val tableName = s"DynamodbEventSourceSpec_${System.currentTimeMillis}"
    implicit val TaskToTask = scalaz.NaturalTransformation.refl[Task]

    val key = Column[String]("key")
    val seq = Column[Long]("seq")
    val value = Column[String]("value").column

    lazy val tableDefinition =
      TableDefinition.from[String, String, String, Long](tableName, key.column, value, key, seq)

    class MyDAO extends DAO[Task](tableDefinition)

    class DBEventStoreAPI[F[_]](val store: Storage[F])(implicit val M: Monad[F], val C: Catchable[F]) extends API[F]

    lazy val eventStore = new MyDAO

    lazy val eventSourceApi = new DBEventStoreAPI[Task](eventStore)
  }

  import DBEventSource._
  import Operation.syntax._

  implicit lazy val runner = TaskTransformation.runner(DYNAMO_CLIENT)

  implicit val JodaDateTimeEqual: Equal[DateTime] =
    Equal.equalBy { _.withZone(DateTimeZone.UTC).toInstant.getMillis }

  implicit val EventEqual: Equal[Event] = Equal.equal { (a, b) =>
    a.id == b.id && implicitly[Equal[DateTime]].equal(a.time, b.time) &&
      a.operation == b.operation
  }

  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  def is = stopOnFail ^ s2"""
    This is a specification to check the DynamoDB event source for blob mappings

    DynamoEventSource.Events should                      ${step(startLocalDynamoDB)} ${step(createTestTable)}
       correctly put an event                            ${putEventWorks.set(minTestsOk = NUM_TESTS)}
       return error when saving a duplicate event        ${eventReturnsErrorForDuplicateEvent.set(minTestsOk = NUM_TESTS)}
       return the correct number of events (no paging)   ${nonPagingGetWorks.set(minTestsOk = NUM_TESTS)}
       return the correct number of events (with paging) ${if (IS_LOCAL) pagingGetWorks.set(minTestsOk = 1) else skipped("SKIPPED - not run in AWS integration mode because it is slow")}

                                                         ${step(deleteTestTable)}
                                                         ${step(stopLocalDynamoDB)}

  """

  def pagingGetWorks =
    Prop.forAll { (nonEmptyKey: UniqueString) =>
      // Generate a really long hash to max out item size
      val str = (1 to 12000).toList.map { _ => 'a' }.mkString
      val key = nonEmptyKey.unwrap
      val valueToSave = str
      (1 to 150).foreach { i =>
        eventSourceApi.save(key, valueToSave.insertOp).run
      }
      eventStore.get(key).runFoldMap { _ => 1 }.attemptRun match {
        case \/-(count) => count === 150
        case _          => ko
      }

    }

  def nonPagingGetWorks =
    Prop.forAll { (nonEmptyKey: UniqueString, v1: String, v2: String, v3: String) =>
      val values = List(v1, v2, v3)
      val key = nonEmptyKey.unwrap
      // Save events
      (for {
        _ <- eventSourceApi.save(key, v1.insertOp.ifAbsent)
        _ <- eventSourceApi.save(key, v2.insertOp)
        _ <- eventSourceApi.save(key, v3.insertOp)
      } yield ()).run

      // Make sure we get the right number of events and the value is correct
      val r: Task[Int] = eventStore.get(key).runFoldMap { _ => 1 }
      (r.attemptRun match {
        case \/-(eventCount) => eventCount === 3
        case _               => ko
      }) and (eventSourceApi.get(key).attemptRun match {
        case \/-(saved) => saved === Some(v3)
        case _          => ko
      })
    }

  def putEventWorks =
    Prop.forAll { (nonEmptyKey: UniqueString, value: String) =>
      import eventStore._
      import scalaz.std.option._
      val eventId = EventId.first(nonEmptyKey.unwrap)
      val event = Event(eventId, DateTime.now, Transform.Insert(value))
      eventStore.put(event).run
      DynamoDB.get[EventId, Event](eventId)(tableName, Columns.eventId, Columns.event) must returnValue(Some(event))
    }

  def eventReturnsErrorForDuplicateEvent =
    Prop.forAll { (nonEmptyKey: UniqueString, value: String) =>
      val eventId = EventId.first(nonEmptyKey.unwrap)
      val event = Event(eventId, DateTime.now, Transform.Insert(value))
      (for {
        _ <- eventStore.put(event)
        result <- eventStore.put(event)
      } yield result).attemptRun match {
        case \/-(-\/(e)) => e === DuplicateEvent
        case _           => ko
      }
    }

  def createTestTable() =
    DynamoDBOps.createTable(tableDefinition)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(tableDefinition)
}
