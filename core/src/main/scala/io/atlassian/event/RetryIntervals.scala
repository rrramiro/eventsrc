package io.atlassian.event

import scala.concurrent.duration._
import scalaz.effect.IO

trait RetryTypes {
  type RetryIntervals = IO[Seq[Duration]]
}

object RetryIntervals {
  /**
   * Provides a RetryIntervals that backs off exponentially but randomly adds jitter over the full interval. Should perform
   * nicely for our event source use case as described in http://www.awsarchitectureblog.com/2015/03/backoff.html
   */
  def fullJitter(retryLimit: Int, base: Duration, backoffFactor: Double): RetryIntervals =
    if (retryLimit <= 0 || base <= 0.millis || backoffFactor < 1)
      throw new IllegalArgumentException(s"Parameters cannot be negative")
    else
      IO {
        (1 to retryLimit).map { i =>
          base * Math.pow(i.toDouble - 1, backoffFactor) * scala.util.Random.nextDouble()
        }
      }
}
