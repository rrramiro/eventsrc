package io.atlassian.event.stream.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.atlassian.aws.dynamodb.Write.Mode.Insert
import io.atlassian.aws.dynamodb._
import io.atlassian.event.Sequence
import io.atlassian.event.stream.{Event, EventId, EventStorage, EventStream}
import org.joda.time.DateTime

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.monad._

class DynamoEventStorage[F[_], KK, S: Sequence, E](awsClient: AmazonDynamoDBClient, tableDef: TableDefinition[KK, E, KK, S])(
  implicit M: Monad[F],
  C: Catchable[F],
  runAction: DynamoDBAction ~> Task,
  ToF: Task ~> F) extends EventStorage[F, KK, S, E] {

  type EID = EventId[KK, S]
  object EID {
    def apply(k: KK, s: S): EID = EventId[KK, S](k, s)
    def unapply(e: EID): Option[(KK, S)] = EventId.unapply[KK, S](e)
  }
  type EV = Event[KK, S, E]

  object table extends Table {
    type K = EID
    type V = EV
    type H = KK
    type R = S
  }

  object Columns {
    val eventId = Column.compose2[EID](tableDef.hash, tableDef.range) { case EID(k, s) => (k, s) } { case (k, s) => EventId(k, s) }
    val lastModified = Column[DateTime]("LastModifiedTimestamp")

    val event = Column.compose3[EV](eventId.liftOption, lastModified, tableDef.value) {
      case Event(id, ts, tx) => (None, ts, tx)
    } {
      case (Some(id), ts, tx) => Event(id, ts, tx)
      case (None, _, _) => ??? // Shouldn't happen, it means there is no event Id in the row
    }
  }

  val interpret: table.DBAction ~> Task =
    runAction compose
      table.transform(DynamoDB.interpreter(table)(
        TableDefinition.from(tableDef.name, Columns.eventId, Columns.event, tableDef.hash, tableDef.range)(tableDef.hash.decoder, tableDef.range.decoder)))


  override def get(key: KK, fromSeq: Option[S]): Process[F, Event[KK, S, E]] = {

    import Process._

    def requestPage(q: table.Query): Task[Page[table.R, EV]] = {
      Task.suspend {
        interpret(table.query(q))
      }
    }

    def loop(pt: Task[Page[table.R, EV]]): Process[Task, EV] =
      await(pt) { page =>
        emitAll(page.result) ++ {
          page.next.fold(halt: Process[Task, EV]) { seq =>
            loop(requestPage(table.Query.range(key, seq, Comparison.Gt))) }
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

  override def put(event: Event[KK, S, E]): F[EventStream.Error \/ Event[KK, S, E]] =
    for {
      putResult <- ToF { interpret(table.putIfAbsent(event.id, event)) }

      r <- putResult match {
        case Insert.New => event.right.point[F]
        case Insert.Failed => EventStream.Error.DuplicateEvent.left[EV].point[F]
      }
    } yield r
}
