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
import DynamoDBAction._

case class WrappedKey[KK](key: KK, dummy: String)

object WrappedKey {
  def onlyKey[KK](kk: KK) = WrappedKey(kk, "")
}

case class DynamoSingleSnapshotStorage[F[_], KK, S, VV](
    tableDefinition: TableDefinition[WrappedKey[KK], Snapshot[S, VV], KK, String],
    snapshotStore: SnapshotStorage[F, KK, S, VV])

object DynamoSingleSnapshotStorage {
  /**
   * Basic implementation of snapshot storage using Dynamo for persistence. It stores only a single snapshot
   * that is overwritten over time.
   */
  def fromDefinition[F[_]: Applicative, KK: Encoder: Decoder, S, VV](tableDef: TableDefinition[KK, VV, KK, S], runAction: DynamoDBAction ~> F) = {
    object Columns {
      import SnapshotType._

      implicit val SnapshotTypeEncoder: Encoder[SnapshotType] =
        Encoder[String].contramap { SnapshotType.apply }

      implicit val SnapshotTypeDecoder: Decoder[SnapshotType] =
        Decoder[String].collect(Function.unlift(SnapshotType.unapply))

      val dummy = Column[String]("_")
      val wrappedKey =
        Column.compose2[WrappedKey[KK]](tableDef.key, dummy.column) {
          wrappedKey => (wrappedKey.key, wrappedKey.dummy)
        } {
          case (key, _) => WrappedKey.onlyKey(key)
        }
      val snapshotDateTime = Column[DateTime]("created").column
      val snapshotType = Column[SnapshotType]("type").column

      val value = Column.compose4[Snapshot[S, VV]](snapshotType, tableDef.value.liftOption, tableDef.range.column.liftOption, snapshotDateTime.liftOption) {
        case Snapshot.NoSnapshot()   => (NoSnapshot, None, None, None)
        case Snapshot.Deleted(s, t)  => (Deleted, None, s.some, t.some)
        case Snapshot.Value(v, s, t) => (Value, v.some, s.some, t.some)
      } {
        case (NoSnapshot, _, _, _)              => Snapshot.zero[S, VV]
        case (Deleted, _, Some(s), Some(t))     => Snapshot.deleted(s, t)
        case (Value, Some(v), Some(s), Some(t)) => Snapshot.value(v)(s, t)
        case _                                  => ???
      }
    }

    val tableDefinition =
      TableDefinition.from(tableDef.name, Columns.wrappedKey, Columns.value, tableDef.hash, Columns.dummy)

    val snapshotStore = new SnapshotStorage[F, KK, S, VV] {
      private[dynamo] object table extends Table {
        type K = WrappedKey[KK]
        type V = Snapshot[S, VV]
        type H = KK
        type R = String
      }

      private val interpret: table.DBAction ~> F =
        runAction compose
          table.transform(DynamoDB.interpreter(table)(tableDefinition))

      override def get(key: KK, sequence: SequenceQuery[S]): F[Snapshot[S, VV]] =
        sequence.fold(
          { _ => Snapshot.zero[S, VV].point[F] },
          Snapshot.zero.point[F],
          interpret.apply(table.get(WrappedKey.onlyKey(key))).map { _ | Snapshot.zero })

      override def put(snapshotKey: KK, snapshot: Snapshot[S, VV], mode: SnapshotStoreMode): F[SnapshotStorage.Error \/ Snapshot[S, VV]] =
        mode.fold(
          snapshot.right.point[F],
          interpret.apply(table.put(WrappedKey.onlyKey(snapshotKey), snapshot)).map { _ => snapshot.right })
    }

    DynamoSingleSnapshotStorage(
      tableDefinition,
      snapshotStore)
  }
}
