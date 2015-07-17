package io.atlassian.event
package stream
package dynamo

import EventStreamError.DuplicateEvent
import source.Transform
import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
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
class DynamoEventStorageSpec(val arguments: Arguments) extends ScalaCheckSpec with LocalDynamoDB with DynamoDBActionMatchers {

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
      case (Transform.Op.Delete, None)    => Transform.delete
      case (Transform.Op.Delete, Some(v)) => Transform.delete // This shouldn't happen because the delete shouldn't have any data
      case (Transform.Op.Insert, None)    => ??? // shouldn't happen
    }

    lazy val tableDefinition =
      TableDefinition.from[KK, E, KK, S](tableName, key.column, transform, key, seq)

  }

  val TaskToTask: Task ~> Task = scalaz.NaturalTransformation.refl[Task]
  lazy val runner: DynamoDBAction ~> Task =
    new (DynamoDBAction ~> Task) {
      def apply[A](a: DynamoDBAction[A]): Task[A] =
        a.run(DYNAMO_CLIENT).fold({ i => Task.fail(WrappedInvalidException.orUnderlying(i)) }, { a => Task.now(a) })
    }

  object DBEventStorage extends DynamoEventStorage[Task, KK, S, E](DynamoMappings.tableDefinition, runner, TaskToTask)

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
    This is a specification to check the DynamoDB event storage

    DynamoEventStorage should                            ${step(startLocalDynamoDB)} ${step(createTestTable)}
       correctly put an event                            ${putEventWorks.set(minTestsOk = NUM_TESTS)}
       return error when saving a duplicate event        ${eventReturnsErrorForDuplicateEvent.set(minTestsOk = NUM_TESTS)}
       return the correct number of events (no paging)   ${nonPagingGetWorks.set(minTestsOk = NUM_TESTS)}
       return the correct number of events (with paging) ${if (IS_LOCAL) pagingGetWorks.set(minTestsOk = 1) else skipped("SKIPPED - not run in AWS integration mode because it is slow")}
       return the correct number of events when a 'from sequence' is specified ${getFromWorks.set(minTestsOk = NUM_TESTS)}

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
        val eventId = EventId[KK, S](nonEmptyKey.unwrap, i.toLong)
        val event = Event[KK, S, E](eventId, DateTime.now, Transform.insert(valueToSave))
        DBEventStorage.put(event).run
      }
      DBEventStorage.get(key, None).runFoldMap { _ => 1 }.attemptRun match {
        case \/-(count) => count === 150
        case _          => ko
      }

    }

  def nonPagingGetWorks =
    Prop.forAll { (nonEmptyKey: UniqueString, v1: String, v2: String, v3: String) =>
      val values = List(v1, v2, v3)
      val key = nonEmptyKey.unwrap

      // Save events
      values.zipWithIndex.foreach {
        case (s, i) =>
          val eventId = EventId[KK, S](nonEmptyKey.unwrap, i.toLong)
          val event = Event[KK, S, E](eventId, DateTime.now, Transform.insert(s))
          DBEventStorage.put(event).run
      }

      // Make sure we get the right number of events and the value is correct
      val r: Task[Int] = DBEventStorage.get(key, None).runFoldMap { _ => 1 }
      val last: Task[Option[String]] = DBEventStorage.get(key, None).runLast.map { _.flatMap { _.operation.value } }
      (r.attemptRun match {
        case \/-(eventCount) => eventCount === 3
        case _               => ko
      }) and (last.attemptRun match {
        case \/-(saved) => saved === Some(v3)
        case _          => ko
      })
    }

  def getFromWorks =
    Prop.forAll { (nonEmptyKey: UniqueString, v1: String, v2: String, v3: String) =>
      val values = List(v1, v2, v3)
      val key = nonEmptyKey.unwrap

      // Save events
      values.zipWithIndex.foreach {
        case (s, i) =>
          val eventId = EventId[KK, S](nonEmptyKey.unwrap, i.toLong)
          val event = Event[KK, S, E](eventId, DateTime.now, Transform.insert(s))
          DBEventStorage.put(event).run
      }

      // Make sure we get the right number of events and the value is correct
      val r: Task[Int] = DBEventStorage.get(key, Some(0)).runFoldMap { _ => 1 }
      val last: Task[List[String]] = DBEventStorage.get(key, Some(0)).runFoldMap { x => List(x.operation.value) }.map { _.flatten }
      (r.attemptRun match {
        case \/-(eventCount) => eventCount === 2
        case _               => ko
      }) and (last.attemptRun match {
        case \/-(saved) => saved === List(v2, v3)
        case _          => ko
      })
    }

  def putEventWorks =
    Prop.forAll { (nonEmptyKey: UniqueString, value: String) =>
      val eventId = EventId.first[KK, S](nonEmptyKey.unwrap)
      val event: DBEventStorage.EV = Event[KK, S, E](eventId, DateTime.now, Transform.insert(value))
      DBEventStorage.put(event).run
      DynamoDB.get[DBEventStorage.EID, DBEventStorage.EV](eventId)(
        DynamoMappings.tableName,
        DBEventStorage.Columns.eventId, DBEventStorage.Columns.event
      ) must returnValue(event.some)
    }

  def eventReturnsErrorForDuplicateEvent =
    Prop.forAll { (nonEmptyKey: UniqueString, value: String) =>
      val eventId = EventId.first(nonEmptyKey.unwrap)
      val event = Event(eventId, DateTime.now, Transform.insert(value))
      (for {
        _ <- DBEventStorage.put(event)
        result <- DBEventStorage.put(event)
      } yield result).attemptRun match {
        case \/-(-\/(e)) => e === DuplicateEvent
        case _           => ko
      }
    }

  def createTestTable() =
    DynamoDBOps.createTable(DynamoMappings.tableDefinition)

  def deleteTestTable() =
    DynamoDBOps.deleteTable(DynamoMappings.tableDefinition)
}
