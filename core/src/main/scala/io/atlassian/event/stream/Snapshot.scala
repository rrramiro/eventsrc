package io.atlassian.event
package stream

import org.joda.time.DateTime
import scalaz.\/

/**
 * A Snapshot wraps an optional value, and tags it with an event Id. We can say a 'snapshot' S of key K at event
 * S.at is value S.value. The value is somehow generated from the event stream (see API.acc)
 *
 * The event Id is quite a useful thing in addition to the value of the snapshot.
 *
 * @tparam V The type of the value wrapped by the Snapshot
 */
sealed trait Snapshot[K, S, V] {
  import Snapshot._

  def value: Option[V]

  def seq: Option[S] =
    this.fold(None, { case (_, seq, _) => Some(seq) }, { case (seq, _) => Some(seq) })

  def fold[X](none: => X, value: (V, S, DateTime) => X, deleted: (S, DateTime) => X): X =
    this match {
      case NoSnapshot() => none
      case Value(v, seq, time) => value(v, seq, time)
      case Deleted(seq, time) => deleted(seq, time)
    }
}

object Snapshot {
  /**
   * There is no snapshot... i.e. no events have been saved.
   */
  case class NoSnapshot[K, S, V]() extends Snapshot[K, S, V] {
    val value = None
  }

  /**
   * Events have been saved and there is a value stored.
   * @param view The value
   * @param s Represents the point in the stream that this Snapshot is for
   */
  case class Value[K, S, V](view: V, s: S, time: DateTime) extends Snapshot[K, S, V] {
    val value = Some(view)
  }

  /**
   * Events have been saved and there is no value (i.e. the value has been deleted).
   * @param s Represents the point in the stream
   */
  case class Deleted[K, S, V](s: S, time: DateTime) extends Snapshot[K, S, V] {
    val value = None
  }

  def zero[K, S, V]: Snapshot[K, S, V] =
    NoSnapshot[K, S, V]()

  def value[K, S, V](view: V): (S, DateTime) => Snapshot[K, S, V] =
    (seq: S, time: DateTime) => Value(view, seq, time)

  def deleted[K, S, V]: (S, DateTime) => Snapshot[K, S, V] =
    (seq: S, time: DateTime) => Deleted(seq, time)

  def noop[K, S, V](old: Snapshot[K, S, V]): (S, DateTime) => Snapshot[K, S, V] =
    (seq: S, time: DateTime) =>
      old.fold(
        deleted[K, S, V],
        { case (v, _, _) => value[K, S, V](v) },
        { case (_, _) => deleted[K, S, V] }
      )(seq, time)
}

