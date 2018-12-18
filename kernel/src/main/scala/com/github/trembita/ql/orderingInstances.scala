package com.github.trembita.ql

import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.concurrent.duration._
import GroupingCriteria._
import AggRes._

trait orderingInstances extends Serializable {
  implicit val localDateOrdering: Ordering[LocalDate] =
    Ordering.fromLessThan(_ isBefore _)
  implicit val localTimeOrdering: Ordering[LocalTime] =
    Ordering.fromLessThan(_ isBefore _)
  implicit val localDateTimeOrdering: Ordering[LocalDateTime] =
    Ordering.fromLessThan(_ isBefore _)
  implicit val durationOrdering: Ordering[FiniteDuration] =
    Ordering.fromLessThan(_ <= _)

  implicit def taggedOrdering[A: Ordering, U]: Ordering[A :@ U] =
    Ordering.by(_.value)

  implicit object GNilOrdering extends Ordering[GNil] {
    def compare(x: GNil, y: GNil): Int = 0
  }
  implicit def groupingCriteriaOrdering[GH <: :@[_, _], GT <: GroupingCriteria](
      implicit GH: Ordering[GH],
      GT: Ordering[GT]
  ): Ordering[GH &:: GT] = new Ordering[GH &:: GT] {
    def compare(x: GH &:: GT, y: GH &:: GT): Int =
      GH.compare(x.head, y.head) match {
        case 0     => GT.compare(x.tail, y.tail)
        case other => other
      }
  }

  implicit object RNilOrdering extends Ordering[RNil] {
    def compare(x: RNil, y: RNil): Int = 0
  }
  implicit def aggResOrdering[RH <: :@[_, _], RT <: AggRes](
      implicit GH: Ordering[RH],
      GT: Ordering[RT]
  ): Ordering[RH *:: RT] = new Ordering[RH *:: RT] {
    def compare(x: RH *:: RT, y: RH *:: RT): Int =
      GH.compare(x.head, y.head) match {
        case 0     => GT.compare(x.tail, y.tail)
        case other => other
      }
  }
}
