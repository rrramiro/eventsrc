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
 * @tparam A The type of the value wrapped by the Snapshot
 */
sealed trait Snapshot[S, A] {
  import Snapshot._

  def value: Option[A]

  def seq: Option[S] =
    this.fold(None, { case (_, seq, _) => Some(seq) }, { case (seq, _) => Some(seq) })

  def fold[X](none: => X, value: (A, S, DateTime) => X, deleted: (S, DateTime) => X): X =
    this match {
      case NoSnapshot()        => none
      case Value(v, seq, time) => value(v, seq, time)
      case Deleted(seq, time)  => deleted(seq, time)
    }

  def map[B](f: A => B): Snapshot[S, B] =
    fold[Snapshot[S, B]](
      zero,
      { case (a, s, d) => Value(f(a), s, d) },
      { case (s, d) => deleted[S, B](s, d) })
}

object Snapshot {
  /** There is no snapshot... i.e. no events have been saved. */
  case class NoSnapshot[S, A]() extends Snapshot[S, A] {
    val value = None
  }

  /**
   * Events have been saved and there is a value stored.
   * @param view The value
   * @param s Represents the point in the stream that this Snapshot is for
   */
  case class Value[S, A](view: A, s: S, time: DateTime) extends Snapshot[S, A] {
    val value = Some(view)
  }

  /**
   * Events have been saved and there is no value (i.e. the value has been deleted).
   * @param s Represents the point in the stream
   */
  case class Deleted[S, A](s: S, time: DateTime) extends Snapshot[S, A] {
    val value = None
  }

  def zero[S, A]: Snapshot[S, A] =
    NoSnapshot[S, A]()

  def value[S, A](view: A): (S, DateTime) => Snapshot[S, A] =
    (seq: S, time: DateTime) => Value(view, seq, time)

  def deleted[S, A]: (S, DateTime) => Snapshot[S, A] =
    (seq: S, time: DateTime) => Deleted(seq, time)

  def noop[K, S, A](old: Snapshot[S, A]): (S, DateTime) => Snapshot[S, A] =
    (seq: S, time: DateTime) =>
      old.fold(
        deleted[S, A],
        { case (v, _, _) => value[S, A](v) },
        { case (_, _) => deleted[S, A] })(seq, time)
}

