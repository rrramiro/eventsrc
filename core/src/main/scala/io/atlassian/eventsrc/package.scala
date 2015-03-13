package io.atlassian

import scalaz._

package object eventsrc extends EventTypes {
  implicit class TaggedOps[A, T](val a: A @@ T) extends AnyVal {
    def unwrap: A = Tag.unwrap(a)
  }
}
