package io.atlassian.event
package stream

import org.joda.time.{ DateTime, DateTimeZone }
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import scalaz.{ \/, Catchable, Monad, OptionT, Order, Semigroup, State }

case class EventState[KK, S, E, A](run: State[List[Event[KK, S, E]], A])

object EventState {
  implicit def eventStateMonad[KK, S, E]: Monad[EventState[KK, S, E, ?]] =
    new Monad[EventState[KK, S, E, ?]] {
      def point[A](a: => A): EventState[KK, S, E, A] =
        EventState(a.point[State[List[Event[KK, S, E]], ?]])

      def bind[A, B](fa: EventState[KK, S, E, A])(f: A => EventState[KK, S, E, B]): EventState[KK, S, E, B] =
        EventState(fa.run.flatMap(f(_).run))
    }

  // scalaz-stream makes us do these gross things.
  implicit def eventStateCatchable[KK, S, E]: Catchable[EventState[KK, S, E, ?]] =
    new Catchable[EventState[KK, S, E, ?]] {
      def attempt[A](f: EventState[KK, S, E, A]): EventState[KK, S, E, Throwable \/ A] =
        EventState(f.run.map(_.right))

      def fail[A](err: Throwable): EventState[KK, S, E, A] =
        throw err
    }
}

case class TestEventLog[KK, S, E](list: List[Event[KK, S, E]])

object TestEventLog {
  def genEvent[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Gen[Event[KK, S, E]] =
    for {
      key <- arbitrary[KK]
      seq <- arbitrary[S]
      instant <- Gen.posNum[Long]
      operation <- arbitrary[E]
    } yield Event(EventId(key, seq), new DateTime(instant, DateTimeZone.UTC), operation)

  def nubBy[A](l: List[A])(f: (A, A) => Boolean): List[A] =
    l.foldRight(List.empty[A])((a, b) => a :: b.filter(!f(a, _)))

  implicit def arbitraryTestLog[KK: Arbitrary, S: Order: Arbitrary, E: Arbitrary]: Arbitrary[TestEventLog[KK, S, E]] =
    Arbitrary(
      for {
        list <- Gen.listOf(genEvent[KK, S, E])
        sorted = list.sortBy(_.id.seq)(Order[S].toScalaOrdering)
        unique = nubBy(sorted)(_.id.seq === _.id.seq)
      } yield TestEventLog(unique)
    )
}

class EventStorageSpec extends SpecificationWithJUnit with ScalaCheck {
  def is =
    s2"""
         EventStorage properties

         EventStorage Semigroup is idempotent       ${eventStorageIdempotenceLaw[Int, String]}
    """

  def storage[KK, S, E]: EventStorage[EventState[KK, S, E, ?], KK, S, E] =
    new EventStorage[EventState[KK, S, E, ?], KK, S, E] {
      def get(key: KK, fromSeq: Option[S]): Process[EventState[KK, S, E, ?], Event[KK, S, E]] =
        Process.await[EventState[KK, S, E, ?], List[Event[KK, S, E]], Event[KK, S, E]] {
          EventState(State.get[List[Event[KK, S, E]]])
        } {
          Process.emitAll[Event[KK, S, E]]
        }

      def put(event: Event[KK, S, E]): EventState[KK, S, E, EventStreamError \/ Event[KK, S, E]] =
        EventState[KK, S, E, EventStreamError \/ Event[KK, S, E]] {
          State.modify[List[Event[KK, S, E]]](event :: _) as event.right
        }

      def latest(key: KK): OptionT[EventState[KK, S, E, ?], Event[KK, S, E]] =
        OptionT[EventState[KK, S, E, ?], Event[KK, S, E]] {
          EventState[KK, S, E, Option[Event[KK, S, E]]] {
            State.gets[List[Event[KK, S, E]], Option[Event[KK, S, E]]] { _.headOption }
          }
        }
    }

  // Scala fails to find this instance of the EventStorage.eventStorageSemigroup implicit without help.
  implicit def eventStorageSemigroup[E, S: Order]: Semigroup[EventStorage[EventState[Unit, S, E, ?], Unit, S, E]] =
    EventStorage.eventStorageSemigroup[EventState[Unit, S, E, ?], Unit, S, E]

  def getStorage[E, S](log: TestEventLog[Unit, S, E])(s: EventStorage[EventState[Unit, S, E, ?], Unit, S, E]): List[Event[Unit, S, E]] =
    s.get((), None).runLog.map { _.toList }.run.eval(log.list)

  def eventStorageIdempotenceLaw[S: Order: Arbitrary, E: Arbitrary] =
    Prop.forAll { (log: TestEventLog[Unit, S, E]) =>
      val result = storage[Unit, S, E] |+| storage[Unit, S, E]
      val expected = storage[Unit, S, E]
      val go = getStorage(log) _
      go(result) === go(expected)
    }
}
