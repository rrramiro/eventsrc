package io.atlassian.event
package source
package dynamo

import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClient => DynamoClient }
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException

import io.atlassian.aws.dynamodb.{ Column, Comparison, Decoder, Encoder, Page, Table, TableDefinition, ValueUpdate }
import io.atlassian.aws.OverwriteMode
import kadai.{ Attempt, Invalid }
import org.joda.time.DateTime
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

trait DynamoEventSource[KK, VV, S] extends EventSource[KK, VV, S] {
  import DynamoEventSource._
  import EventSource.Error

  object table extends Table {
    type K = EventId
    type V = Event
    type H = KK
    type R = S
  }

  abstract class DAO[F[_]](awsClient: DynamoClient, tableDef: TableDefinition[KK, VV, KK, S])(
    implicit M: Monad[F],
    C: Catchable[F],
    interpret: table.DBAction ~> Task ,
    ToF: Task ~> F) extends Storage[F] {

    object Columns {
      implicit val transformOpDecoder: Decoder[Transform.Op] =
        Decoder[String].mapPartial(Function.unlift(Transform.Op.unapply))
      implicit val transformOpEncoder: Encoder[Transform.Op] =
        Encoder[String].contramap(Transform.Op.apply)


      val eventId = Column.compose2[EventId](tableDef.key, tableDef.range) { case EventId(k, s) => (k, s) } { case (k, s) => EventId(k, s) }
      val lastModified = Column[DateTime]("LastModifiedTimestamp")
      val transform = Column.compose2[Transform[VV]](Column[Transform.Op]("Operation"), tableDef.value.liftOption) {
        case Transform.Delete => (Transform.Op.Delete, None)
        case Transform.Insert(v) => (Transform.Op.Insert, Some(v))
      } {
        case (Transform.Op.Insert, Some(v)) => Transform.Insert(v)
        case (Transform.Op.Delete, None) => Transform.delete
        //case _ => ??? // shouldn't happen
      }

      val event = Column.compose3[Event](eventId, lastModified, transform) { case Event(id, ts, tx) => (id, ts, tx) } { case (id, ts, tx) => Event(id, ts, tx) }
    }

    import scalaz.syntax.monad._

    /**
     * To return a stream of events from Dynamo, we first need to execute a query, then emit results, and then optionally
     * recursively execute the next query until there is nothing more to query.
     *
     * @param key The key
     * @return Stream of events.
     */
    override def get(key: KK, fromSeq: Option[S]): Process[F, Event] = {

      import Process._

      def requestPage(q: table.Query): Task[Page[table.R, Event]] = {
        Task.suspend {
          interpret(table.query(q))
        }
      }

      def loop(pt: Task[Page[table.R, Event]]): Process[Task, Event] =
        await(pt) { page =>
          emitAll(page.result) ++ {
            page.next.fold(halt: Process[Task, Event]) { seq => loop(requestPage(table.Query.range(key, seq, Comparison.Gte))) }
          }
        }

      loop {
        requestPage {
          fromSeq.fold {
            table.Query.hash(key)
          } {
            seq => table.Query.range(key, seq, Comparison.Gte)
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
        putResult <- ToF { interpret(table.put(event.id, event)) }

        r <- putResult match {
          case \/-(c) => c.right.point[F]
          case -\/(Invalid.Err(d: ConditionalCheckFailedException)) => Error.DuplicateEvent.left[Event].point[F]
          case -\/(i) => C.fail(toThrowable(i))
        }
      } yield r

    val EventDynamoValue: ValueUpdate[Event] =
      ValueUpdate.withUpdated {
        (_, a) => StoreValue.newFromValues(a)
      }
  }
}
