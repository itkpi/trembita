package com.datarootlabs.trembita.chronos

import java.time._
import scala.concurrent.duration._


trait LocalDateTimeOps extends Any {
  def self: LocalDateTime

  def toMillis: Long = self.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

  def isBetween(start: LocalDateTime, end: LocalDateTime): Boolean = self.isAfter(start) && self.isBefore(end)

  def <=<?(t: (LocalDateTime, LocalDateTime)): Boolean = isBetween(t._1, t._2)

  def diff(other: LocalDateTime): FiniteDuration = (other.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli - toMillis).millis

  def --(other: LocalDateTime): FiniteDuration = diff(other)

  def minus(duration: FiniteDuration): LocalDateTime = self.minusNanos(duration.toNanos)

  def plus(duration: FiniteDuration): LocalDateTime = self.plusNanos(duration.toNanos)

  def /+(duration: FiniteDuration): LocalDateTime = plus(duration)

  def /-(duration: FiniteDuration): LocalDateTime = minus(duration)
}
