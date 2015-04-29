package io.atlassian.event

import org.scalacheck.Arbitrary._
import org.scalacheck.{ Arbitrary, Gen }

import scalaz._

trait DataArbitraries {
  type NonEmpty
  type NonEmptyString = String @@ NonEmpty
  implicit val ArbitraryNonEmptyString: Arbitrary[NonEmptyString] =
    Arbitrary { arbitrary[String].filter { !_.isEmpty }.map { Tag.apply } }

  type UniqueStringMarker
  type UniqueString = String @@ UniqueStringMarker
  implicit val ArbitraryUniqueString: Arbitrary[UniqueString] =
    Arbitrary { Gen.uuid map { _.toString } map { Tag.apply } }

}
