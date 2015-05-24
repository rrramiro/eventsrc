package io.atlassian.event

import scala.concurrent.duration._
import scalaz.effect.IO

trait RetryTypes {
  type Retry = IO[Seq[Duration]]
}

object Retry {
  def exponentialBackoffWithJitter(retryLimit: Int, initialInterval: Duration, backoffFactor: Double): Option[Retry] =
    if (retryLimit <= 0 || initialInterval <= 0.millis || backoffFactor < 0)
      None
    else
      Some {
        IO {
          (1 to retryLimit).map { i =>
            initialInterval * Math.pow(i.toDouble - 1, backoffFactor) + (initialInterval * scala.util.Random.nextDouble())
          }
        }
      }
}