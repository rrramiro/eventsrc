package io.atlassian.event
package stream
package dynamo

import io.atlassian.aws.dynamodb._
import DirectoryEventStream.DirectoryId

import kadai.Attempt
import scalaz.Isomorphism.<=>
import scodec.bits.ByteVector

case class ColumnDirectoryId(did: DirectoryId) extends AnyVal

object ColumnDirectoryId {
  val iso = new (ColumnDirectoryId <=> DirectoryId) {
    def from = (did: DirectoryId) => ColumnDirectoryId(did)
    def to = (cdid: ColumnDirectoryId) => cdid.did
  }

  implicit val DirectoryIdEncoder: Encoder[ColumnDirectoryId] =
    Encoder[String].contramap { _.did.id }

  implicit val DirectoryIdDecoder: Decoder[ColumnDirectoryId] =
    Decoder[String].map { did => ColumnDirectoryId(DirectoryId(did)) }
}

case class ColumnTwoPartSequence(tps: TwoPartSequence[Long]) extends AnyVal

object ColumnTwoPartSequence {
  val iso = new (ColumnTwoPartSequence <=> TwoPartSequence[Long]) {
    def from = (tps: TwoPartSequence[Long]) => ColumnTwoPartSequence(tps)
    def to = (ctps: ColumnTwoPartSequence) => ctps.tps
  }

  implicit def TwoPartSequenceEncoder: Encoder[ColumnTwoPartSequence] =
    Encoder[NonEmptyBytes].contramap { seq =>
      ByteVector.fromLong(seq.tps.seq) ++ ByteVector.fromLong(seq.tps.zone) match {
        case NonEmptyBytes(b) => b
      }
    }

  implicit def TwoPartSequenceDecoder: Decoder[ColumnTwoPartSequence] =
    Decoder[NonEmptyBytes].mapAttempt { bytes =>
      if (bytes.bytes.length != 16)
        Attempt.fail(s"Invalid length of byte vector ${bytes.bytes.length}")
      else
        Attempt.safe {
          bytes.bytes.splitAt(8) match {
            case (abytes, bbytes) => ColumnTwoPartSequence(TwoPartSequence(abytes.toLong(), bbytes.toLong()))
          }
        }
    }
}

import SingleStreamExample._

case class ColumnSingleStreamKey(ssk: SingleStreamKey)

object ColumnSingleStreamKey {
  val iso = new (ColumnSingleStreamKey <=> SingleStreamKey) {
    def from = (ssk: SingleStreamKey) => ColumnSingleStreamKey(ssk)
    def to = (cssk: ColumnSingleStreamKey) => cssk.ssk
  }

  implicit val SingleStreamKeyEncoder: Encoder[ColumnSingleStreamKey] =
    Encoder[String].contramap { _.ssk.unwrap }

  implicit val SingleStreamKeyDecoder: Decoder[ColumnSingleStreamKey] =
    Decoder[String].map { ssk => ColumnSingleStreamKey(SingleStreamKey(ssk)) }
}
