package io.atlassian.event
package stream
package unsafe

import org.scalacheck.{ Arbitrary, Prop }
import org.specs2.matcher.DisjunctionMatchers

import scala.collection.concurrent.TrieMap
import scalaz.{ Equal, \/- }
import scalaz.std.anyVal._
import scalaz.std.string._

class UnsafeRewritableInMemoryEventStorageSpec extends ScalaCheckSpec with DisjunctionMatchers {
  import TestEventStorage._

  def is =
    s2"""
         This spec tests that events can be rewritten in memory

         Can rewrite an event ${canRewriteASingleEvent[Boolean, Long, String]}
    """

  def canRewriteASingleEvent[KK: Equal: Arbitrary, S: Sequence: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (oldTestEvent: TestEvent[KK, S, E], newOperation: E) =>
      val oldEvent = oldTestEvent.event
      val storage = TrieMap[KK, List[Event[KK, S, E]]](oldEvent.id.key -> List(oldEvent))
      val newEvent = oldEvent.copy(operation = newOperation)
      val result = UnsafeRewritableInMemoryEventStorage(storage).unsafeRewrite(oldEvent, newEvent).run

      result must be_\/-(newEvent) and (storage.get(oldEvent.id.key) must beSome(List(newEvent)))
    }
}
