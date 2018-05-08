package com.datarootlabs.trembita.chronos

import language.higherKinds
import java.time._
import scala.concurrent.duration._


trait LocalDateOps extends Any {
  def self: LocalDate

  def toMillis: Long = self.atTime(LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

  def isBetween(start: LocalDate, end: LocalDate): Boolean = self.isAfter(start) && self.isBefore(end)

  def <=<?(t: (LocalDate, LocalDate)): Boolean = isBetween(t._1, t._2)

  def diff(other: LocalDate): FiniteDuration = (other.atTime(LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli - toMillis).millis

  def --(other: LocalDate): FiniteDuration = diff(other)

  def minus(duration: FiniteDuration): LocalDate = self.minusDays(duration.toDays)

  def plus(duration: FiniteDuration): LocalDate = self.plusDays(duration.toDays)

  def /+(duration: FiniteDuration): LocalDate = plus(duration)

  def /-(duration: FiniteDuration): LocalDate = minus(duration)

  def to(that: LocalDate): Vector[LocalDate] = {
    val builder = Vector.newBuilder[LocalDate]
    var now = self
    while (now isBefore that) {
      builder += now
      now = now.plusDays(1)
    }
    if (self isBefore that) builder += that
    builder.result()
  }

  def untilDate(that: LocalDate): Vector[LocalDate] = {
    val builder = Vector.newBuilder[LocalDate]
    var now = self
    while (now.isBefore(that)) {
      builder += now
      now = now.plusDays(1)
    }
    builder.result()
  }
}
