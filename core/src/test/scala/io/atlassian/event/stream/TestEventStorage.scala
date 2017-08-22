package io.atlassian.event
package stream

import io.atlassian.event.Reason
import org.joda.time.{ DateTime, DateTimeZone }
import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Arbitrary.arbitrary

import scalaz.{ Equal, NonEmptyList, OptionT, Order, \/ }
import scalaz.syntax.all._
import scalaz.stream.Process

case class TestEvent[KK, S, E](event: Event[KK, S, E])

object TestEvent {
  def genEvent[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Gen[Event[KK, S, E]] =
    for {
      key <- arbitrary[KK]
      seq <- arbitrary[S]
      instant <- Gen.posNum[Long]
      operation <- arbitrary[E]
    } yield Event(EventId(key, seq), new DateTime(instant, DateTimeZone.UTC), operation)

  implicit def arbitraryTestEvent[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Arbitrary[TestEvent[KK, S, E]] =
    Arbitrary(genEvent[KK, S, E].map(TestEvent[KK, S, E]))
}

case class TestEventStorage[KK, S, E](
  underlyingGet: List[Event[KK, S, E]],
  run: EventStorage[SafeCatchable, KK, S, E]
)

object TestEventStorage {
  val genReason: Gen[Reason] =
    arbitrary[String].map(Reason.apply)

  val genEventStreamError: Gen[EventStreamError] =
    Gen.oneOf[EventStreamError](
      EventStreamError.duplicate,
      for {
        head <- genReason
        tail <- Gen.listOf(genReason)
      } yield EventStreamError.reject(NonEmptyList(head, tail: _*))
    )

  def genGet[KK: Arbitrary, S: Order: Arbitrary, E: Arbitrary]: Gen[List[Event[KK, S, E]]] =
    for {
      list <- Gen.listOf(TestEvent.genEvent[KK, S, E])
      sorted = EventStorageSpec.orderBy(list)(_.id.seq)
      unique = EventStorageSpec.nubBy(sorted)(_.id.seq === _.id.seq)
    } yield unique

  def genPut[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Gen[SafeCatchable[EventStreamError \/ Event[KK, S, E]]] =
    Gen.frequency[SafeCatchable[EventStreamError \/ Event[KK, S, E]]](
      1 -> SafeCatchable.failure[EventStreamError \/ Event[KK, S, E]],
      20 -> Gen.frequency[EventStreamError \/ Event[KK, S, E]](
        1 -> genEventStreamError.map(_.left),
        20 -> TestEvent.genEvent[KK, S, E].map(_.right)
      ).map(SafeCatchable.success)
    )

  def genLatest[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Gen[OptionT[SafeCatchable, Event[KK, S, E]]] =
    Gen.frequency(
      1 -> OptionT.none[SafeCatchable, Event[KK, S, E]],
      20 -> Gen.frequency[SafeCatchable[Event[KK, S, E]]](
        1 -> SafeCatchable.failure[Event[KK, S, E]],
        20 -> TestEvent.genEvent[KK, S, E].map(SafeCatchable.success)
      ).map(_.liftM[OptionT])
    )

  implicit def arbitraryTestEventStorage[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Arbitrary]: Arbitrary[TestEventStorage[KK, S, E]] =
    Arbitrary(
      for {
        g <- genGet[KK, S, E]
        p <- genPut[KK, S, E]
        l <- genLatest[KK, S, E]
      } yield TestEventStorage(
        g,
        new EventStorage[SafeCatchable, KK, S, E] {
          override def get(key: KK, fromSeq: Option[S]): Process[SafeCatchable, Event[KK, S, E]] =
            Process.emitAll(g.filter(_.id.key === key))

          override def put(event: Event[KK, S, E]): SafeCatchable[EventStreamError \/ Event[KK, S, E]] =
            p

          override def latest(key: KK): OptionT[SafeCatchable, Event[KK, S, E]] =
            l
        }
      )
    )
}
