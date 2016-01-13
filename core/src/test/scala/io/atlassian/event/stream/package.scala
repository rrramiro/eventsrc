package io.atlassian.event

import scalaz.concurrent.Task
import scalaz.effect.LiftIO

package object stream {
  implicit val taskLiftIO: LiftIO[Task] = TaskLiftIO
}
