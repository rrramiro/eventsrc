package io.atlassian.event

import scala.concurrent.duration._
import scalaz.effect.IO

trait RetryInterval {
  def next: IO[Option[(Duration, RetryInterval)]]
}

object RetryInterval {
  def fullJitter(retryLimit: Int, base: Duration, backoffFactor: Double): RetryInterval =
    if (retryLimit <= 0 || base <= 0.millis || backoffFactor < 1)
      throw new IllegalArgumentException(s"Parameters cannot be negative")
    else
      new FullJitterRetryInterval(retryLimit, base, backoffFactor, 0)
}

private class FullJitterRetryInterval(retryLimit: Int, base: Duration, backoffFactor: Double, current: Int) extends RetryInterval {
  override def next: IO[Option[(Duration, RetryInterval)]] = {
    val nextDouble = IO { scala.util.Random.nextDouble() }
    nextDouble.map { randomDouble =>
      if (current == retryLimit)
        None
      else
        Some((base * Math.pow(current.toDouble + 1, backoffFactor) * randomDouble,
          new FullJitterRetryInterval(retryLimit, base, backoffFactor, current + 1)))
    }
  }
}
