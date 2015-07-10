package io.atlassian.event

import org.specs2.execute.AsResult
import org.specs2.matcher.{ ResultMatchers, TaskMatchers }
import org.specs2.{ ScalaCheck, SpecificationWithJUnit }
import scalaz.concurrent.Task

trait ScalaCheckSpec extends SpecificationWithJUnit with ScalaCheck with DataArbitraries with TaskMatchers with ResultMatchers {
  def taskTest[T: AsResult](t: => Task[T]) =
    t must returnValue(beSuccessful[T])
}
