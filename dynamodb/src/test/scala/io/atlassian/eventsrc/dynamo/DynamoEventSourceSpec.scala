package io.atlassian.eventsrc
package dynamo

import org.junit.runner.RunWith
import org.specs2.main.Arguments
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }
import io.atlassian.aws.dynamodb.{ AttributeDefinition, Column, DynamoDB, DynamoDBSpecOps, LocalDynamoDBSpec, Marshaller, TableDefinition, Unmarshaller }
import org.specs2.specification.Step
import scalaz.syntax.id._
import scalaz.std.anyVal._
import scalaz.Scalaz.Id
import scalaz.concurrent.Task
import org.scalacheck.Prop
import org.joda.time.DateTime
import scalaz.{ Equal, -\/, \/- }
import io.atlassian.eventsrc.EventSource.Error.DuplicateEvent
import io.atlassian.eventsrc.{ Operation, Sequence, Transform }

@RunWith(classOf[org.specs2.runner.JUnitRunner])
class DynamoEventSourceSpec(val arguments: Arguments) extends SpecificationWithJUnit with LocalDynamoDBSpec {

  object DBEventSource extends DynamoEventSource[String, String, Long] with LongSequencedEventSource[String, String] {
    val tableName = s"DynamodbEventSourceSpec_${System.currentTimeMillis}"
    implicit val TaskToTask = scalaz.NaturalTransformation.refl[Task]
    val key = Column[String]("key")
    val marshallKey = Marshaller.fromColumn[String](key)
    val seq = Column[Long]("seq")
    val marshallSeq = Marshaller.fromColumn[Long](seq)
    val marshallEventId = Marshaller.fromColumn2[String, Long, EventId](key, seq) {
      case EventId(key, sequence) => (key, sequence)
    }
    val deleted = Column[Option[DateTime]]("DeletedTimestamp")
    val lastModified = Column[DateTime]("LastModifiedTimestamp")
    val operation = Column[Transform.Op]("Operation")
    val marshallEvent = Marshaller.from[Event] {
      case Event(EventId(k, sequence), time, op) =>
        Map(lastModified(time), key(k), seq(sequence)) ++ {
          op match {
            case Transform.Insert(v) => Marshaller[String].toMap(v) ++ Marshaller.fromColumn(operation).toMap(Transform.Op.Insert)
            case Transform.Delete    => Marshaller.fromColumn(operation).toMap(Transform.Op.Delete)
          }
        }
    }

    val unmarshallEvent =
      Unmarshaller.from[Event](
        for {
          lastModified <- lastModified.get
          op <- operation.get
          key <- key.get
          seq <- seq.get
          tx <- op match {
            case Transform.Op.Insert => Unmarshaller[String].unmarshall.flatMap { m => Operation.ok(Insert(m)) }
            case Transform.Op.Delete => Operation.ok[Transform[String]](Delete)
          }
        } yield Event(EventId(key, seq), lastModified, tx)
      )

    lazy val eventStore = new DAO[Task](dynamoClient, tableName, new Marshallers(
      key,
      seq,
      marshallKey,
      marshallSeq,
      marshallEventId,
      marshallEvent,
      unmarshallEvent
    )) {}

    implicit def EventEqual: Equal[Event] = Equal.equal { (a, b) =>
      a.id == b.id && implicitly[Equal[DateTime]].equal(a.time, b.time) &&
        a.operation == b.operation
    }

    //    implicit val EventIdUnmarshaller =
    //      Unmarshaller.from[EventId] {
    //        for {
    //          key <- Unmarshaller.Operation.get[String](key.name)
    //          seq <- Unmarshaller.Operation.get[Long](seq.name)
    //        } yield EventId(key, seq)
    //      }

    implicit val tableMapping =
      TableDefinition.from[String, String](tableName, key.name, Some(AttributeDefinition.number(seq.name)))

    def createTestTable() =
      createTable[String, String](tableMapping, dynamoClient)

    def deleteTestTable() =
      deleteTable[EventId, Event](tableMapping, dynamoClient)
  }

  import DBEventSource._
  import Operation.syntax._

  // For some reason this particular spec is run with the working directory of key-mapper, not blobstore, so
  // it can't find the scripts.
  override val scriptDirectory = s"../install-scripts/local"
  implicit val DYNAMO_CLIENT = dynamoClient

  val NUM_TESTS =
    if (IS_LOCAL) 100
    else 10

  def is = stopOnFail ^ s2"""
    This is a specification to check the DynamoDB event source for blob mappings

    DynamoEventSource.Events should                   ${Step(startLocalDynamoDB)} ${Step(createTestTable)}
       correctly put an event                            ${putEventWorks.set(minTestsOk = NUM_TESTS)}
       return error when saving a duplicate event        ${eventReturnsErrorForDuplicateEvent.set(minTestsOk = NUM_TESTS)}
       return the correct number of events (no paging)   ${nonPagingGetWorks.set(minTestsOk = NUM_TESTS)}
       return the correct number of events (with paging) ${if (IS_LOCAL) pagingGetWorks.set(minTestsOk = 1) else skipped("SKIPPED - not run in AWS integration mode because it is slow")}

  """

  def pagingGetWorks =
    Prop.forAll { (key: MappedKey, v: BlobMapping) =>
      import eventStore.{ EventIdKeyMarshaller, EventMarshaller, EventUnmarshaller, tableMapping }

      // Generate a really long hash to max out item size
      val str = (1 to 12000).toList.map { _ => 'a' }.mkString
      val valueToSave = v.copy(metaData = Some(str))
      (1 to 150).foreach { i =>
        eventSourceApi.save(key, valueToSave.insertOp).run
      }
      eventStore.get(key).runFoldMap { _ => 1 }.run match {
        case \/-(count) => count === 150
        case _          => ko
      }

    }

  def nonPagingGetWorks =
    Prop.forAll { (key: MappedKey, v1: BlobMapping, v2: BlobMapping, v3: BlobMapping) =>
      val values = List(v1, v2, v3)

      // Save events
      (for {
        _ <- eventSourceApi.save(key, v1.insertOp.ifAbsent)
        _ <- eventSourceApi.save(key, v2.insertOp)
        _ <- eventSourceApi.save(key, v3.insertOp)
      } yield ()).run

      // Make sure we get the right number of events and the value is correct
      val r: Result[Int] = eventStore.get(key).runFoldMap { _ => 1 }
      (r.run match {
        case \/-(eventCount) => eventCount === 3
        case _               => ko
      }) and (eventSourceApi.get(key).run match {
        case \/-(saved) => saved === Some(v3)
        case _          => ko
      })
    }

  def putEventWorks =
    Prop.forAll { (key: MappedKey, value: BlobMapping) =>
      import eventStore._
      import scalaz.std.option._
      val eventId = EventId.first(key)
      val event = Event(eventId, DateTime.now, Transform.Insert(value))
      eventStore.put(event).run
      DynamoDB.get[EventId, Event](eventId) must returnValue(Some(event))
    }

  def eventReturnsErrorForDuplicateEvent =
    Prop.forAll { (key: MappedKey, value: BlobMapping) =>
      val eventId = EventId.first(key)
      val event = Event(eventId, DateTime.now, Transform.Insert(value))
      (for {
        _ <- eventStore.put(event)
        result <- eventStore.put(event)
      } yield result).run match {
        case \/-(-\/(e)) => e === DuplicateEvent
        case _           => ko
      }
    }
}
