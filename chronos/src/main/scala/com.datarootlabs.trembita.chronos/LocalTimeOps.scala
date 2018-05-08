package com.datarootlabs.trembita.chronos

import java.time._
import scala.concurrent.duration._

trait LocalTimeOps extends Any {
  def self: LocalTime

  def isBetween(begin: LocalTime, end: LocalTime): Boolean = self.isAfter(begin) && self.isBefore(end)

  def <=<?(t: (LocalTime, LocalTime)): Boolean = isBetween(t._1, t._2)

  def diff(other: LocalTime): FiniteDuration = java.time.Duration.between(self, other).toMillis.millis

  def --(other: LocalTime): FiniteDuration = diff(other)
}
