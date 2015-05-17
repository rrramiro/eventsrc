package io.atlassian.event
package stream
package dynamo

import io.atlassian.aws.dynamodb._
import org.joda.time.DateTime

import scalaz.{ Applicative, ~>, \/ }
import scalaz.syntax.either._
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.syntax.monad._

/**
 * Basic implementation of snapshot storage using Dynamo for persistence. It stores only a single snapshot
 * that is overwritten over time.
 */
class DynamoSingleSnapshotStorage[F[_]: Applicative, KK, S, VV](tableDef: TableDefinition[KK, VV, KK, S])(implicit runAction: DynamoDBAction ~> F)
    extends SnapshotStorage[F, KK, S, VV] {

  private[dynamo] case class WrappedKey(key: KK, dummy: String = "")

  private[dynamo] object table extends Table {
    type K = WrappedKey
    type V = Snapshot[S, VV]
    type H = KK
    type R = String
  }

  private[dynamo] object Columns {
    import SnapshotType._

    implicit val SnapshotTypeEncoder: Encoder[SnapshotType] =
      Encoder[String].contramap { SnapshotType.apply }

    implicit val SnapshotTypeDecoder: Decoder[SnapshotType] =
      Decoder[String].collect(Function.unlift(SnapshotType.unapply))

    val dummy = Column[String]("_")
    val wrappedKey =
      Column.compose2[WrappedKey](tableDef.key, dummy) {
        wrappedKey => (wrappedKey.key, wrappedKey.dummy)
      } {
        case (key, _) => WrappedKey(key)
      }
    val snapshotDateTime = Column[DateTime]("created")
    val snapshotType = Column[SnapshotType]("type")

    val value = Column.compose4[Snapshot[S, VV]](snapshotType, tableDef.value.liftOption, tableDef.range.liftOption, snapshotDateTime.liftOption) {
      case Snapshot.NoSnapshot() => (NoSnapshot, None, None, None)
      case Snapshot.Deleted(s, t) => (Deleted, None, s.some, t.some)
      case Snapshot.Value(v, s, t) => (Value, v.some, s.some, t.some)
    } {
      case (NoSnapshot, _, _, _) => Snapshot.zero[S, VV]
      case (Deleted, _, Some(s), Some(t)) => Snapshot.deleted(s, t)
      case (Value, Some(v), Some(s), Some(t)) => Snapshot.value(v)(s, t)
      case _ => ???
    }
  }

  private[dynamo] val tableDefinition =
    TableDefinition.from(tableDef.name, Columns.wrappedKey, Columns.value, tableDef.hash, Columns.dummy)(tableDef.hash.decoder, Columns.dummy.decoder)

  private val interpret: table.DBAction ~> F =
    runAction compose
      table.transform(DynamoDB.interpreter(table)(tableDefinition))

  override def get(key: KK, sequence: SequenceQuery[S]): F[Snapshot[S, VV]] =
    sequence.fold({ _ => Snapshot.zero[S, VV].point[F] },
      Snapshot.zero.point[F],
      interpret(table.get(WrappedKey(key))).map { _ | Snapshot.zero }
    )

  override def put(snapshotKey: KK, snapshot: Snapshot[S, VV], mode: SnapshotStoreMode): F[SnapshotStorage.Error \/ Snapshot[S, VV]] =
    mode.fold(snapshot.right.point[F],
      interpret(table.put(WrappedKey(snapshotKey), snapshot)).map { _ => snapshot.right }
    )

}
