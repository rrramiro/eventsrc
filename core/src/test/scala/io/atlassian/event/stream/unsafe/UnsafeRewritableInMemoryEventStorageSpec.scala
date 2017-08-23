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

         Can rewrite an event        ${canRewriteASingleEvent[Boolean, Long, String]}
         Will only rewrite one event ${onlyRewritesOneEvent[Boolean, Long, String]}
         Will fail on bad seq #      ${failsWritingNonExistentSeq[Boolean, Long, String]}
    """

  def canRewriteASingleEvent[KK: Equal: Arbitrary, S: Sequence: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (oldTestEvent: TestEvent[KK, S, E], newOperation: E) =>
      val oldEvent = oldTestEvent.event
      val newEvent = oldEvent.copy(operation = newOperation)
      val storage = TrieMap[KK, List[Event[KK, S, E]]](oldEvent.id.key -> List(oldEvent))
      val result = UnsafeRewritableInMemoryEventStorage(storage).unsafeRewrite(oldEvent, newEvent).run

      result must be_\/-(newEvent) and (storage.get(oldEvent.id.key) must beSome(List(newEvent)))
    }

  def onlyRewritesOneEvent[KK: Equal: Arbitrary, S: Sequence: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (op1: TestEvent[KK, S, E], op2: E, newOperation: E) =>
      val event1 = op1.event
      val event2 = op1.event.copy(id = EventId.next(op1.event.id), operation = op2)
      val rewritten = op1.event.copy(operation = newOperation)

      val storage = TrieMap[KK, List[Event[KK, S, E]]](event1.id.key -> List(event1, event2))
      val result = UnsafeRewritableInMemoryEventStorage(storage).unsafeRewrite(event1, rewritten).run

      result must be_\/-(rewritten) and (storage.get(event1.id.key) must beSome(List(rewritten, event2)))
    }

  def failsWritingNonExistentSeq[KK: Equal: Arbitrary, S: Sequence: Arbitrary, E: Equal: Arbitrary] =
    Prop.forAll { (op1: TestEvent[KK, S, E], op2: E, newOperation: E) =>
      val event1 = op1.event
      val event2 = op1.event.copy(id = EventId.next(op1.event.id), operation = op2)
      val rewritten = op1.event.copy(id = EventId.next(event2.id), operation = newOperation)

      val storage = TrieMap[KK, List[Event[KK, S, E]]](event1.id.key -> List(event1, event2))
      val result = UnsafeRewritableInMemoryEventStorage(storage).unsafeRewrite(event1, rewritten).run

      result must be_-\/ and (storage.get(event1.id.key) must beSome(List(event1, event2)))
    }
}
