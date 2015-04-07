package io.atlassian.event
package stream

import scalaz.\/
import scalaz.stream.Process

/**
 * A source of events. Implementations wrap around an underlying data store (e.g. in-memory map or DynamoDB).
 *
 * @tparam F Container around operations on an underlying data store e.g. Task.
 */
trait EventStorage[F[_], K, S, E] {
  /**
   * Retrieve a stream of events from the underlying data store. This stream should take care of pagination and
   * cleanup of any underlying resources (e.g. closing connections if required).
   * @param key The key
   * @param fromSeq The starting sequence to get events from. None to get from the start.
   * @return Stream of events.
   */
  def get(key: K, fromSeq: Option[S] = None): Process[F, Event[K, S, E]]

  /**
   * Save the given event.
   *
   * @return Either an Error or the event that was saved. Other non-specific errors should be available
   *         through the container F.
   */
  def put(event: Event[K, S, E]): F[EventStream.Error \/ Event[K, S, E]]
}
