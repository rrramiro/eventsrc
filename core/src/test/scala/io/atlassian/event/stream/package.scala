package io.atlassian.event

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ThreadFactory, Executors }

import scalaz.concurrent.Task
import scalaz.effect.LiftIO

package object stream {
  implicit val taskLiftIO: LiftIO[Task] = TaskLiftIO

  val DefaultExecutor =
    Executors.newScheduledThreadPool(1, new ThreadFactory {
      val id = new AtomicInteger()

      override def newThread(r: Runnable): Thread =
        new Thread(r, "test/eventstream-scheduled-"+id.getAndIncrement)
    })
}
