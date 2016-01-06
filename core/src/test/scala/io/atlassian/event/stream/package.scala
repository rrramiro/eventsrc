package io.atlassian.event

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ ThreadFactory, Executors }

package object stream {
  val DefaultExecutor =
    Executors.newScheduledThreadPool(1, new ThreadFactory {
      val id = new AtomicInteger()

      override def newThread(r: Runnable): Thread =
        new Thread(r, "test/eventstream-scheduled-"+id.getAndIncrement)
    })
}
