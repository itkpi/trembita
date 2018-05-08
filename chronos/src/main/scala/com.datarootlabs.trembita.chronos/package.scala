package com.datarootlabs.trembita

import language.implicitConversions
import java.time.{Duration => JDuration, _}
import contextual._
import scala.concurrent.duration._


package object chronos {
  implicit class ChronoInterpolators(val sc: StringContext) extends AnyVal {
    def t = Prefix(LocalTimeInterpolator, sc)
    def d = Prefix(LocalDateInterpolator, sc)
    def dt = Prefix(LocalDateTimeInterpolator, sc)
    def zdt = Prefix(ZonedDateTimeInterpolator, sc)
  }

  implicit class TimeOps(val self: LocalTime) extends AnyVal with LocalTimeOps
  implicit class DateOps(val self: LocalDate) extends AnyVal with LocalDateOps
  implicit class DateTimeOps(val self: LocalDateTime) extends AnyVal with LocalDateTimeOps

  implicit def javaDuration2ScalaDuration(jDuration: JDuration): FiniteDuration = jDuration.toMillis.millis
}
