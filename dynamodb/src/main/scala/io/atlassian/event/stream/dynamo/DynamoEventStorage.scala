package io.atlassian.event.stream.dynamo

import io.atlassian.aws.dynamodb.DynamoDB.ReadConsistency
import io.atlassian.aws.dynamodb.Write.Mode.Insert
import io.atlassian.aws.dynamodb.Write.Mode.Replace
import io.atlassian.aws.dynamodb._
import io.atlassian.event.stream.{ Event, EventId, EventStorage, EventStreamError }
import org.joda.time.DateTime

import scalaz._
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.monad._
import DynamoDBAction._

/**
 * Implementation of EventStorage using DynamoDB via the aws-scala library. To use it:
 *   1. Provide a table definition. Specifically, this requires an event stream key NamedColumn, a Column definition
 *   for the events, and a NamedColumn for the sequence type. This in turn will require Decoders/Encoders to be
 *   defined for the key, sequence and event columns.
 *   2. Provide an interpreter for DynamoDBActions to Task. DynamoDBActions are created inside and need to be run.
 *   3. Provide a transform from Task to your desired container type. Task is actually a good option since it needs a
 *   Monad and Catchable, so the transform can just be a NaturalTransformation.refl[Task].
 *
 * @tparam F Container around operations on an underlying data store e.g. Task.
 */
class DynamoEventStorage[F[_], KK, S, E](
    tableDef: TableDefinition[KK, E, KK, S],
    runAction: DynamoDBAction ~> F,
    queryConsistency: ReadConsistency
)(
    implicit
    M: Monad[F],
    EKK: Encoder[KK],
    DKK: Decoder[KK],
    ES: Encoder[S],
    DS: Decoder[S],
    C: Catchable[F]
) extends EventStorage[F, KK, S, E] {

  lazy val columns = EventSourceColumns(tableDef.key, tableDef.hash, tableDef.range, tableDef.value)

  type EID = columns.EID
  type EV = columns.EV

  private object table extends Table {
    type K = EID
    type V = EV
    type H = KK
    type R = S
  }

  private val interpret: table.DBAction ~> F =
    runAction compose
      table.transform(DynamoDB.interpreter(table)(schema))

  lazy val schema =
    TableDefinition.from[EID, EV, KK, S](tableDef.name, columns.eventId, columns.event, tableDef.hash, tableDef.range)

  override def get(key: KK, fromSeq: Option[S]): Process[F, Event[KK, S, E]] = {
    import Process._

    def requestPage(q: table.Query): F[Page[table.R, EV]] =
      interpret(table.query(q.config(table.Query.Config(consistency = queryConsistency))))

    def loop(pt: F[Page[table.R, EV]]): Process[F, EV] =
      await(pt) { page =>
        emitAll(page.result) ++ {
          page.next.fold(halt: Process[F, EV]) { seq =>
            loop(requestPage(table.Query.range(key, seq, Comparison.Gt)))
          }
        }
      }

    loop {
      requestPage {
        fromSeq.fold {
          table.Query.hash(key)
        } {
          seq => table.Query.range(key, seq, Comparison.Gt)
        }
      }
    }
  }

  override def put(event: Event[KK, S, E]): F[EventStreamError \/ Event[KK, S, E]] =
    for {
      putResult <- interpret(table.putIfAbsent(event.id, event))

      r <- putResult match {
        case Insert.New    => event.right.point[F]
        case Insert.Failed => EventStreamError.DuplicateEvent.left[EV].point[F]
      }
    } yield r

  override def rewriteEvent(oldEvent: Event[KK, S, E], newEvent: Event[KK, S, E]): F[EventStreamError \/ Event[KK, S, E]] =
    for {
      replaceResult <- interpret(table.replace(oldEvent.id, oldEvent, newEvent))
      r <- replaceResult match {
        case Replace.Wrote  => newEvent.right.point[F]
        case Replace.Failed => EventStreamError.EventChanged.left[EV].point[F]
      }
    } yield r

  def latest(key: KK): OptionT[F, Event[KK, S, E]] = {
    def runQuery[A](q: table.Query, f: Page[S, EV] => Option[A]) =
      OptionT(interpret(table.query(q).map(f)))

    val config = table.Query.Config(direction = ScanDirection.Descending, limit = Some(1), consistency = queryConsistency)
    val hashQuery = table.Query.hash(key, config)
    runQuery(hashQuery, _.result.headOption)
  }
}
