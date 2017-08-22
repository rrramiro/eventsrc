package io.atlassian.event
package stream

import org.joda.time.{ DateTime, DateTimeZone }
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{ Arbitrary, Gen, Prop }
import org.specs2.matcher.{ BeTypedEqualTo, Matcher }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scalaz.{ Applicative, Catchable, Equal, Monad, Monoid, NonEmptyList, OptionT, Order, Traverse, \/ }
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.all._
import scalaz.stream.Process

object EventStorageSpec {
  def nubBy[A](l: List[A])(f: (A, A) => Boolean): List[A] =
    l.foldRight(List.empty[A])((a, b) => a :: b.filter(!f(a, _)))

  def orderBy[A, B: Order](xs: List[A])(f: A => B): List[A] =
    xs.sorted(Order[B].contramap[A](f).toScalaOrdering)
}

class EventStorageSpec extends SpecificationWithJUnit with ScalaCheck {
  import TestEventStorage._

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
