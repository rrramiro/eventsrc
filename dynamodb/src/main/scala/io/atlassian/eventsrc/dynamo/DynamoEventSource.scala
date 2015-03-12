package io.atlassian.eventsrc
package dynamo

import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClient => DynamoClient }
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException

import io.atlassian.aws.dynamodb.{ Column, Comparison, Decoder, DynamoDB, Encoder, Marshaller, Page, Query, StoreValue, TableDefinition, Unmarshaller }
import io.atlassian.aws.OverwriteMode
import kadai.{ Attempt, Invalid }
import scalaz.{ Catchable, Monad, Show, \/, \/-, -\/, ~> }
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._

object DynamoEventSource {
  private val toThrowable: Invalid => Throwable = {
    case Invalid.Err(t) => t
    case i              => new RuntimeException(Show[Invalid].shows(i))
  }
}

trait DynamoEventSource[K, V, S] extends EventSource[K, V, S] {
  import DynamoEventSource._
  import EventSource.Error

  /**
   * Pass in the marshallers and unmarshallers to turn our data into dynamo maps
   */
  case class Marshallers(
    keyCol: Column[K],
    seqCol: Column[S],
    marshallKey: Marshaller[K],
    marshallSeq: Marshaller[S],
    marshallEventId: Marshaller[EventId],
    marshallEvent: Marshaller[Event],
    unmarshall: Unmarshaller[Event])

  abstract class DAO[F[_]](awsClient: DynamoClient, tableName: String, marshallers: Marshallers)(
    implicit M: Monad[F],
    C: Catchable[F],
    ToF: Task ~> F) extends Storage[F] {

    import scalaz.syntax.monad._
    implicit val keyCol: Column[K] = marshallers.keyCol
    implicit val seqCol: Column[S] = marshallers.seqCol
    implicit val marshallKey: Marshaller[K] = marshallers.marshallKey
    implicit val marshallSeq: Marshaller[S] = marshallers.marshallSeq
    implicit val marshallEventId: Marshaller[EventId] = marshallers.marshallEventId
    implicit val marshallEvent: Marshaller[Event] = marshallers.marshallEvent
    implicit val unmarshall: Unmarshaller[Event] = marshallers.unmarshall

    implicit def tableDef: TableDefinition[EventId, Event]

    implicit def TransformOpEncode: Encoder[Transform.Op] =
      Encoder[String].contramap { Transform.Op.apply }
    implicit def TransformOpDecode: Decoder[Transform.Op] =
      Decoder[String].flatMap {
        _ match {
          case Transform.Op(op) => Decoder.ok(op)
          case op               => Decoder.from { Attempt.fail(s"Invalid operation: $op") }
        }
      }

    /**
     * To return a stream of events from Dynamo, we first need to execute a query, then emit results, and then optionally
     * recursively execute the next query until there is nothing more to query.
     *
     * @param key The key
     * @return Stream of events.
     */
    override def get(key: K, fromSeq: Option[S]): Process[F, Event] = {

      import Process._

      def requestPage(q: Query[Event]): Task[Page[Event]] = Task.suspend {
        DynamoDB.query(q).run(awsClient).run.fold(i => Task.fail(toThrowable(i)), Task.now)
      }

      def loop(pt: Task[Page[Event]]): Process[Task, Event] =
        await(pt) { page =>
          emitAll(page.result) ++ {
            page.next.fold(halt: Process[Task, Event])(nextQuery => loop(requestPage(nextQuery)))
          }
        }

      loop {
        requestPage {
          fromSeq.fold {
            Query.forHash[K, EventId, Event](key)
          } {
            seq => Query.forHashAndRange[K, S, EventId, Event](key, seq, Comparison.Gte)
          }
        }
      }.translate(ToF)
    }

    /**
     * To save an event, we need to enable OverwriteMode.NoOverwrite
     * and also catch ConditionalCheckFailedException, which represents a duplicate event.
     *
     * @param event The event to save.
     * @return Either an EventSource.Error or the event that was saved. Other non-specific errors should be available
     *         through the container F.
     */
    override def put(event: Event): F[Error \/ Event] =
      for {
        putResult <- DynamoDB.put[EventId, Event](event.id, event, OverwriteMode.NoOverwrite).map { _ => event }.run(awsClient).run.point[F]

        r <- putResult match {
          case \/-(c) => c.right.point[F]
          case -\/(Invalid.Err(d: ConditionalCheckFailedException)) => Error.DuplicateEvent.left[Event].point[F]
          case -\/(i) => C.fail(toThrowable(i))
        }
      } yield r

    implicit val EventDynamoValue: StoreValue[Event] =
      StoreValue.withUpdated {
        (_, a) => StoreValue.newFromValues(a)
      }
  }
}
