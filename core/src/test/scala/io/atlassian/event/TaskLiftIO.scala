package io.atlassian.event

import scalaz.concurrent.Task
import scalaz.effect.{ IO, LiftIO }

object TaskLiftIO extends LiftIO[Task] {
  override def liftIO[A](ioa: IO[A]): Task[A] = Task.delay { ioa.unsafePerformIO }
}
