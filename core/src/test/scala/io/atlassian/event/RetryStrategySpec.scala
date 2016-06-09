package io.atlassian.event

import java.util.concurrent.atomic.AtomicInteger

import org.scalacheck.{ Gen, Arbitrary, Prop }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }

import scala.concurrent.duration.Duration
import scalaz.concurrent.Task

class RetryStrategySpec extends SpecificationWithJUnit with ScalaCheck {
  implicit val taskLiftIO = TaskLiftIO

  def is =
    s2"""
         This spec tests RetryStrategy

         Evaluated correct number of times for duration list   $correctEvaluationCountDurationList
         Evaluated correct number of times for retry intervals $correctEvaluationCountRetryInterval
      """

  def correctEvaluationCountDurationList = Prop.forAll { (retryCount: ReasonableParameters) =>
    val counter = new AtomicInteger(0)
    def foo(i: Int): Task[Int] =
      Task.delay {
        counter.incrementAndGet()
      }

    Retry(foo, RetryStrategy.durationList[Task](List.fill(retryCount.count)(Duration(1, "ms")), Delays.sleep), { (_: Int) => true }).unsafePerformSync === retryCount.count + 1
  }

  def correctEvaluationCountRetryInterval = Prop.forAll { (retryCount: ReasonableParameters) =>
    val counter = new AtomicInteger(0)
    def foo(i: Int): Task[Int] =
      Task.delay {
        counter.incrementAndGet()
      }

    Retry(foo, RetryStrategy.retryIntervals[Task](RetryInterval.fullJitter(retryCount.count, Duration(1, "ms"), 1.0), Delays.sleep), { (_: Int) => true }).unsafePerformSync === retryCount.count + 1
  }

  case class ReasonableParameters(count: Int)
  implicit val ReasonableParametersArb: Arbitrary[ReasonableParameters] =
    Arbitrary {
      for {
        count <- Gen.chooseNum(1, 10)
      } yield ReasonableParameters(count)
    }

}
