package io.atlassian.event
package stream

import org.joda.time.{ DateTime, DateTimeZone }
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.specs2.matcher.{ BeTypedEqualTo, Matcher }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }
import scalaz._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.stream.Process
import scalaz.syntax.either._
import scalaz.syntax.monadPlus._
import scalaz.syntax.monoid._
import scalaz.syntax.order._

sealed trait SafeCatchable[A] {
  def toOption: Option[A]
  def get(implicit M: Monoid[A]): A
}

case class CatchableSuccess[A](value: A) extends SafeCatchable[A] {
  def toOption: Option[A] =
    Some(value)

  def get(implicit M: Monoid[A]): A =
    value
}

case class CatchableFailure[A]() extends SafeCatchable[A] {
  def toOption: Option[A] =
    None

  def get(implicit M: Monoid[A]): A =
    mzero[A]
}

object SafeCatchable {
  implicit val safeCatchableMonadPlus: MonadPlus[SafeCatchable] =
    new MonadPlus[SafeCatchable] {
      def point[A](a: => A): SafeCatchable[A] =
        CatchableSuccess(a)

      def bind[A, B](fa: SafeCatchable[A])(f: A => SafeCatchable[B]): SafeCatchable[B] =
        fa match {
          case CatchableSuccess(value) => f(value)
          case CatchableFailure()      => CatchableFailure()
        }

      def empty[A]: SafeCatchable[A] =
        CatchableFailure[A]()

      def plus[A](a: SafeCatchable[A], b: => SafeCatchable[A]): SafeCatchable[A] =
        a match {
          case CatchableSuccess(value) => CatchableSuccess(value)
          case CatchableFailure()      => b
        }
    }

  implicit val safeCatchableCatchable: Catchable[SafeCatchable] =
    new Catchable[SafeCatchable] {
      def attempt[A](f: SafeCatchable[A]): SafeCatchable[Throwable \/ A] =
        f.map(_.right)

      def fail[A](err: Throwable): SafeCatchable[A] =
        mempty[SafeCatchable, A]
    }

  implicit def safeCatchableEqual[A: Equal]: Equal[SafeCatchable[A]] =
    new Equal[SafeCatchable[A]] {
      def equal(a1: SafeCatchable[A], a2: SafeCatchable[A]) =
        (a1, a2) match {
          case (CatchableSuccess(v1), CatchableSuccess(v2)) => v1 === v2
          case (CatchableFailure(), CatchableFailure())     => true
          case (_, _)                                       => false
        }
    }
}

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
      1 -> CatchableFailure[EventStreamError \/ Event[KK, S, E]](),
      20 -> Gen.frequency[EventStreamError \/ Event[KK, S, E]](
        1 -> genEventStreamError.map(_.left),
        20 -> TestEvent.genEvent[KK, S, E].map(_.right)
      ).map(CatchableSuccess.apply)
    )

  def genRewrite[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Gen[SafeCatchable[EventStreamError \/ Event[KK, S, E]]] =
    Gen.frequency[SafeCatchable[EventStreamError \/ Event[KK, S, E]]](
      1 -> CatchableFailure[EventStreamError \/ Event[KK, S, E]](),
      20 -> Gen.frequency[EventStreamError \/ Event[KK, S, E]](
        1 -> genEventStreamError.map(_.left),
        20 -> TestEvent.genEvent[KK, S, E].map(_.right)
      ).map(CatchableSuccess.apply)
    )

  def genLatest[KK: Arbitrary, S: Arbitrary, E: Arbitrary]: Gen[OptionT[SafeCatchable, Event[KK, S, E]]] =
    Gen.frequency(
      1 -> OptionT.none[SafeCatchable, Event[KK, S, E]],
      20 -> Gen.frequency[SafeCatchable[Event[KK, S, E]]](
        1 -> CatchableFailure[Event[KK, S, E]](),
        20 -> TestEvent.genEvent[KK, S, E].map(CatchableSuccess[Event[KK, S, E]])
      ).map(_.liftM[OptionT])
    )

  implicit def arbitraryTestEventStorage[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Arbitrary]: Arbitrary[TestEventStorage[KK, S, E]] =
    Arbitrary(
      for {
        g <- genGet[KK, S, E]
        p <- genPut[KK, S, E]
        r <- genRewrite[KK, S, E]
        l <- genLatest[KK, S, E]
      } yield TestEventStorage(
        g,
        new EventStorage[SafeCatchable, KK, S, E] {
          def get(key: KK, fromSeq: Option[S]): Process[SafeCatchable, Event[KK, S, E]] =
            Process.emitAll(g.filter(_.id.key === key))

          def put(event: Event[KK, S, E]): SafeCatchable[EventStreamError \/ Event[KK, S, E]] =
            p

          def rewrite(oldEvent: Event[KK, S, E], newEvent: Event[KK, S, E]) =
            r

          def latest(key: KK): OptionT[SafeCatchable, Event[KK, S, E]] =
            l
        }
      )
    )
}

object EventStorageSpec {
  def nubBy[A](l: List[A])(f: (A, A) => Boolean): List[A] =
    l.foldRight(List.empty[A])((a, b) => a :: b.filter(!f(a, _)))

  def orderBy[A, B: Order](xs: List[A])(f: A => B): List[A] =
    xs.sorted(Order[B].contramap[A](f).toScalaOrdering)
}

class EventStorageSpec extends SpecificationWithJUnit with ScalaCheck {
  def is =
    s2"""
      EventStorage merge

      EventStorage merge is consistent with filter, nubBy, orderBy  ${getWithLists[Boolean, Int, String]}
      EventStorage merge gives results in ascending order           ${getPreservesOrder[Boolean, Int, String]}
      EventStorage merge gives results without duplicate sequences  ${getNoDuplicates[Boolean, Int, String]}
      EventStorage merge gives equal or more results than input     ${getGreaterResults[Boolean, Int, String]}

      EventStorage Semigroup (associative append)

      EventStorage append is associative with respect to get        ${getSemigroupLaw[Boolean, Int, String]}
      EventStorage append is associative with respect to put        ${putSemigroupLaw[Boolean, Int, String]}
      EventStorage append is associative with respect to latest     ${latestSemigroupLaw[Boolean, Int, String]}

      EventStorage Band (idempotent append)

      EventStorage append is idempotent with respect to get         ${getIdempotenceLaw[Boolean, Int, String]}
      EventStorage append is idempotent with respect to put         ${putIdempotenceLaw[Boolean, Int, String]}
      EventStorage append is idempotent with respect to latest      ${latestIdempotenceLaw[Boolean, Int, String]}
    """

  def equal[A: Equal](expected: A): Matcher[A] =
    new BeTypedEqualTo[A](expected, _ === _)

  def get[F[_]: Catchable: Monad, KK, S, E](key: KK)(es: EventStorage[F, KK, S, E]): F[List[Event[KK, S, E]]] =
    es.get(key, None).runLog.map(_.toList)

  def put[F[_], KK, S, E](event: Event[KK, S, E])(es: EventStorage[F, KK, S, E]): F[EventStreamError \/ Event[KK, S, E]] =
    es.put(event)

  def latest[F[_], KK, S, E](key: KK)(es: EventStorage[F, KK, S, E]): OptionT[F, Event[KK, S, E]] =
    es.latest(key)

  def getWithLists[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], key: KK) =>
      val go = get[SafeCatchable, KK, S, E](key) _
      val xs = s1.underlyingGet ++ s2.underlyingGet
      val filtered = xs.filter(_.id.key === key)
      val unique = EventStorageSpec.nubBy(filtered)(_.id === _.id)
      val sorted = EventStorageSpec.orderBy(unique)((_: Event[KK, S, E]).id.seq)

      sorted must equal(go(s1.run |+| s2.run).get)
    }

  def getPreservesOrder[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], key: KK) =>
      val go = get[SafeCatchable, KK, S, E](key) _

      go(s1.run |+| s2.run).map(EventStorageSpec.orderBy(_)((_: Event[KK, S, E]).id.seq)) must equal(go(s1.run |+| s2.run))
    }

  def getNoDuplicates[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], key: KK) =>
      val go = get[SafeCatchable, KK, S, E](key)(_: EventStorage[SafeCatchable, KK, S, E]).get
      val result = go(s1.run |+| s2.run)

      EventStorageSpec.nubBy(result)(_ === _) must equal(result)
    }

  def getGreaterResults[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], key: KK) =>
      val go = get[SafeCatchable, KK, S, E](key)(_: EventStorage[SafeCatchable, KK, S, E]).get.length

      go(s1.run |+| s2.run) must be_>=(go(s1.run))
      go(s1.run |+| s2.run) must be_>=(go(s2.run))
    }

  def getSemigroupLaw[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], s3: TestEventStorage[KK, S, E], key: KK) =>
      val go = get[SafeCatchable, KK, S, E](key) _

      go((s1.run |+| s2.run) |+| s3.run) must equal(go(s1.run |+| (s2.run |+| s3.run)))
    }

  def putSemigroupLaw[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], s3: TestEventStorage[KK, S, E], event: TestEvent[KK, S, E]) =>
      val go = put[SafeCatchable, KK, S, E](event.event) _

      go((s1.run |+| s2.run) |+| s3.run) must equal(go(s1.run |+| (s2.run |+| s3.run)))
    }

  def latestSemigroupLaw[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (s1: TestEventStorage[KK, S, E], s2: TestEventStorage[KK, S, E], s3: TestEventStorage[KK, S, E], key: KK) =>
      val go = latest[SafeCatchable, KK, S, E](key) _

      go((s1.run |+| s2.run) |+| s3.run) must equal(go(s1.run |+| (s2.run |+| s3.run)))
    }

  def getIdempotenceLaw[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (storage: TestEventStorage[KK, S, E], key: KK) =>
      val a = storage.run
      val go = get[SafeCatchable, KK, S, E](key) _

      go(a) must equal(go(a |+| a))
    }

  def putIdempotenceLaw[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (storage: TestEventStorage[KK, S, E], event: TestEvent[KK, S, E]) =>
      val a = storage.run
      val go = put[SafeCatchable, KK, S, E](event.event) _

      go(a) must equal(go(a |+| a))
    }

  def latestIdempotenceLaw[KK: Equal: Arbitrary, S: Order: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (storage: TestEventStorage[KK, S, E], key: KK) =>
      val a = storage.run
      val go = latest[SafeCatchable, KK, S, E](key) _

      go(a) must equal(go(a |+| a))
    }
}
