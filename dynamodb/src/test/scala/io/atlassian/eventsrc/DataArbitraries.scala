package io.atlassian.eventsrc

import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary._

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
