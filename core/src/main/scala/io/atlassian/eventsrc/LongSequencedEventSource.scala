package io.atlassian.eventsrc

/**
 * Trait that implementations of an event source need to extend where the sequence number is just a long.
 * @tparam K The key against which values are stored.
 * @tparam V Values to be store
 */
trait LongSequencedEventSource[K, V] extends EventSource[K, V, LongSequence] {
  def S: Sequence[LongSequence] = implicitly[Sequence[LongSequence]]
}
