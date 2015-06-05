package io.atlassian.event

import scalaz._
import scalaz.syntax.std.option._

/**
 * Makes it easy to create tagged types with an apply without having to import and deal with `scalaz.Tag` directly.
 * e.g.
 *
 * sealed trait FooMarker
 * type Foo = String @@ FooMarker
 * object Foo extends Tagger[String, FooMarker]
 *
 * ...
 *
 * val foo = Foo("hurrah")
 */
trait Tagger[A] {
  sealed trait Marker
  def apply(a: A): A @@ Marker = Tag(a)

  def unapply(tagged: A @@ Marker): Option[A] = Tag.unwrap(tagged).some
}
