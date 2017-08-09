package io.atlassian.event.stream.unsafe.dynamo

import io.atlassian.aws.dynamodb.DynamoDB.ReadConsistency
import io.atlassian.aws.dynamodb.Write.Mode.Replace
import io.atlassian.aws.dynamodb._
import io.atlassian.event.Reason
import io.atlassian.event.stream.dynamo.EventSourceColumns
import io.atlassian.event.stream.unsafe.RewritableEventStorage
import io.atlassian.event.stream.{ Event, EventStreamError }

import scalaz._
import scalaz.syntax.either._
import scalaz.syntax.monad._

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
class RewritableDynamoEventStorage[F[_], KK, S, E](
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
) extends RewritableEventStorage[F, KK, S, E] {

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

  override def unsafeRewrite(oldEvent: Event[KK, S, E], newEvent: Event[KK, S, E]): F[EventStreamError \/ Event[KK, S, E]] =
    if (oldEvent.id.equals(newEvent.id))
      rewriteEvent(oldEvent, newEvent)
    else
      EventStreamError.reject(NonEmptyList(Reason("The original event changed"))).left[Event[KK, S, E]].point[F]

  def rewriteEvent(oldEvent: Event[KK, S, E], newEvent: Event[KK, S, E]): F[EventStreamError \/ Event[KK, S, E]] =
    for {
      replaceResult <- interpret(table.replace(oldEvent.id, oldEvent, newEvent))
      r <- replaceResult match {
        case Replace.Wrote => newEvent.right.point[F]
        case Replace.Failed =>
          EventStreamError.reject(NonEmptyList(Reason("The supplied events didn't have matching IDs"))).left[EV].point[F]
      }
    } yield r
}
