package io.atlassian.event
package source
package dynamo

import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB => DynamoClient }
import io.atlassian.aws.dynamodb.DynamoDB.ReadConsistency
import io.atlassian.aws.dynamodb.Write.Mode.Insert

import io.atlassian.aws.dynamodb._
import org.joda.time.DateTime
import scalaz.{ Catchable, Monad, \/, ~> }
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._

trait DynamoEventSource[KK, VV, S] extends EventSource[KK, VV, S] {
  import EventSource.Error

  object table extends Table {
    type K = EventId
    type V = Event
    type H = KK
    type R = S
  }

  case class DAOConfig(queryConsistency: ReadConsistency = ReadConsistency.Eventual)

  abstract class DAO[F[_]](tableDef: TableDefinition[KK, VV, KK, S], config: DAOConfig = DAOConfig())(
      implicit
      M: Monad[F],
      C: Catchable[F],
      EKK: Encoder[KK],
      DKK: Decoder[KK],
      ES: Encoder[S],
      DS: Decoder[S],
      runAction: DynamoDBAction ~> Task,
      ToF: Task ~> F
  ) extends Storage[F] {

    object Columns {
      implicit val transformOpDecoder: Decoder[Transform.Op] =
        Decoder[String].collect(Function.unlift(Transform.Op.unapply))
      implicit val transformOpEncoder: Encoder[Transform.Op] =
        Encoder[String].contramap(Transform.Op.apply)

      val eventId = Column.compose2[EventId](tableDef.hash.column, tableDef.range.column) { case EventId(k, s) => (k, s) } { case (k, s) => EventId(k, s) }
      val lastModified = Column[DateTime]("LastModifiedTimestamp").column
      val transform = Column.compose2[Transform[VV]](Column[Transform.Op]("Operation").column, tableDef.value.liftOption) {
        case Transform.Delete => (Transform.Op.Delete, None)
        case Transform.Insert(v) => (Transform.Op.Insert, Some(v))
      } {
        case (Transform.Op.Insert, Some(v)) => Transform.Insert(v)
        case (Transform.Op.Delete, None) => Transform.delete
        case (Transform.Op.Delete, Some(v)) => Transform.delete // This shouldn't happen because the delete shouldn't have any data
        case (Transform.Op.Insert, None) => ??? // shouldn't happen
      }

      val event = Column.compose3[Event](eventId.liftOption, lastModified, transform) {
        case Event(id, ts, tx) => (None, ts, tx)
      } {
        case (Some(id), ts, tx) => Event(id, ts, tx)
        case (None, _, _) => ??? // Shouldn't happen, it means there is no event Id in the row
      }
    }

    val interpret: table.DBAction ~> Task =
      runAction compose
        table.transform(DynamoDB.interpreter(table)(
          TableDefinition.from(tableDef.name, Columns.eventId, Columns.event, tableDef.hash, tableDef.range)
        ))

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
            page.next.fold(halt: Process[Task, Event]) { seq =>
              loop(requestPage(table.Query.range(key, seq, Comparison.Gt)))
            }
          }
        }

      loop {
        requestPage {
          fromSeq.fold {
            table.Query.hash(key, table.Query.Config(consistency = config.queryConsistency))
          } {
            seq => table.Query.range(key, seq, Comparison.Gte, table.Query.Config(consistency = config.queryConsistency))
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
        putResult <- ToF { interpret(table.putIfAbsent(event.id, event)) }

        r <- putResult match {
          case Insert.New => event.right.point[F]
          case Insert.Failed => Error.DuplicateEvent.left[Event].point[F]
        }
      } yield r
  }
}
