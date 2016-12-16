package io.atlassian.event

import org.scalacheck.{ Gen, Prop }

import scalaz.{ \/, -\/, EitherT, NonEmptyList, State, Value }
import scalaz.syntax.traverse._

class UntilFirstLeftSpec extends ScalaCheckSpec {
  def is =
    s2"""
         Specification of untilFirstLeft function

         untilFirstLeft has a single element when given a single element     ${propSingle}
         untilFirstLeft on all right elements is traverse                    ${propAllRightsIsTraverse}
         untilFirstLeft unprocessed list is dropWhile(_.isRight)             ${propUnprocessed}
         untilFirstLeft failure is same as find(_.isLeft)                    ${propFindFailure}
         untilFirstLeft only runs function until a left                      ${propCancels}
    """

  def genEitherUnit[A](g: Gen[A]): Gen[A \/ Unit] =
    g.flatMap(n => Gen.oneOf(\/.left(n), \/.right(())))

  def genNonEmptyList[A](g: Gen[A]): Gen[NonEmptyList[A]] =
    for {
      a <- g
      l <- Gen.listOf(g)
    } yield NonEmptyList.nel(a, l)

  def genLeft[A, B](g: Gen[A]): Gen[A \/ B] =
    g.map(\/.left)

  def genRight[A, B](g: Gen[B]): Gen[A \/ B] =
    g.map(\/.right)

  val genIntDisjunction: Gen[Int \/ Int] =
    Gen.oneOf(genLeft(Gen.posNum[Int]), genRight(Gen.posNum[Int]))

  val genWithAtLeastOneFailure: Gen[NonEmptyList[Int \/ Int]] =
    for {
      init <- genNonEmptyList(genIntDisjunction)
      last <- genLeft(Gen.posNum[Int])
    } yield init append NonEmptyList(last)

  val propSingle: Prop =
    Prop.forAll(genEitherUnit(Gen.posNum[Int])) { p =>
      val expected = p.bimap((_, NonEmptyList(p)), NonEmptyList(_))
      untilFirstLeft(NonEmptyList(p), EitherT.fromDisjunction[Value](_: Int \/ Unit)).run.value mustEqual expected
    }

  val propAllRightsIsTraverse: Prop =
    Prop.forAll(genNonEmptyList(genRight[Int, Int](Gen.posNum[Int]))) { p =>
      val expected = p.sequenceU
      untilFirstLeft(p, EitherT.fromDisjunction[Value](_: Int \/ Int)).run.value mustEqual expected
    }

  val propUnprocessed: Prop =
    Prop.forAll(genWithAtLeastOneFailure) { p =>
      val expected = p.list.dropWhile(_.isRight)
      untilFirstLeft(p, EitherT.fromDisjunction[Value](_: Int \/ Int)).run.value must beLike {
        case -\/((_, l)) => l.list mustEqual expected
      }
    }

  val propFindFailure: Prop =
    Prop.forAll(genWithAtLeastOneFailure) { p =>
      p.list.find(_.isLeft) must beLike {
        case Some(-\/(expected)) =>
          untilFirstLeft(p, EitherT.fromDisjunction[Value](_: Int \/ Int)).run.value must beLike {
            case -\/((e, _)) => e mustEqual expected
          }
      }
    }

  val propCancels: Prop =
    Prop.forAll(genWithAtLeastOneFailure) { p =>
      val expected = p.list.takeWhile(_.isRight).length + 1 // has to run at least once
      type S[A] = State[Int, A]
      untilFirstLeft[NonEmptyList, S, Int \/ Int, Int, Int](p, a => EitherT[S, Int, Int](State.modify((_: Int) + 1).as(a))).run.exec(0) mustEqual expected
    }
}
