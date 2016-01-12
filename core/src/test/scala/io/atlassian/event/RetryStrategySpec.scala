package io.atlassian.event

import java.util.concurrent.atomic.AtomicInteger

import org.scalacheck.{ Gen, Arbitrary, Prop }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scala.concurrent.duration.Duration
import scalaz.concurrent.Task

class RetryStrategySpec extends SpecificationWithJUnit with ScalaCheck {
  def is =
    s2"""
         This spec tests RetryStrategy

         Evaluated correct number of times  $correctEvaluationCount
      """

  def correctEvaluationCount = Prop.forAll { (retryCount: ReasonableParameters) =>
    val counter = new AtomicInteger(0)
    def foo(i: Int): Task[Int] =
      Task.delay {
        counter.incrementAndGet()
      }

    Retry(foo(0), RetryStrategy[Task](Seq.fill(retryCount.count)(Duration(1, "ms"))), { (_: Int) => true }).run === retryCount.count + 1
  }

  case class ReasonableParameters(count: Int)
  implicit val ReasonableParametersArb: Arbitrary[ReasonableParameters] =
    Arbitrary {
      for {
        count <- Gen.chooseNum(1, 1000)
      } yield ReasonableParameters(count)
    }

}
