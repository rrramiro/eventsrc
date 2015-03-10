package io.atlassian.eventsrc
package dynamo

import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClient => DynamoClient }
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException

import io.atlassian.aws.dynamodb.DynamoDB
import io.atlassian.aws.dynamodb.{ Column, Marshaller, Page, Query, StoreValue, TableDefinition, Unmarshaller }
import io.atlassian.aws.OverwriteMode
import kadai.Invalid
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

trait DynamoEventSource[K, V] extends LongSequencedEventSource[K, V] {
  import DynamoEventSource._

  abstract class DAO[F[_]](awsClient: DynamoClient, tableName: String)(
      implicit M: Monad[F],
      C: Catchable[F],
      ToF: Task ~> F,
      keyCol: Column[K],
      marshallKey: Marshaller[K],
      marshallEventId: Marshaller[EventId],
      marshallEvent: Marshaller[Event],
      unmarshall: Unmarshaller[Event]) extends Storage[F] {

    import scalaz.syntax.monad._

    implicit def tableDef: TableDefinition[EventId, Event]

    /**
     * To return a stream of events from Dynamo, we first need to execute a query, then emit results, and then optionally
     * recursively execute the next query until there is nothing more to query.
     *
     * @param key The key
     * @return Stream of events.
     */
    override def get(key: K): Process[F, Event] = {

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

      loop(requestPage(Query.forHash[K, EventId, Event](key))).translate(ToF)
    }

    /**
     * To save an event, we need to enable OverwriteMode.NoOverwrite
     * and also catch ConditionalCheckFailedException, which represents a duplicate event.
     *
     * @param event The event to save.
     * @return Either an EventSourceError or the event that was saved. Other non-specific errors should be available
     *         through the container F.
     */
    override def put(event: Event): F[EventSourceError \/ Event] =
      for {
        putResult <- DynamoDB.put[EventId, Event](event.id, event, OverwriteMode.NoOverwrite).map { _ => event }.run(awsClient).run.point[F]

        r <- putResult match {
          case \/-(c) => c.right.point[F]
          case -\/(Invalid.Err(d: ConditionalCheckFailedException)) => EventSourceError.DuplicateEvent.left[Event].point[F]
          case -\/(i) => C.fail(toThrowable(i))
        }
      } yield r

    implicit val EventDynamoValue: StoreValue[Event] =
      StoreValue.withUpdated {
        (_, a) => StoreValue.newFromValues(a)
      }
  }
}
