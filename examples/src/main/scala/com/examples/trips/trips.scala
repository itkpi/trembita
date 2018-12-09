package com.examples

import java.time._
import io.circe.{Decoder, Encoder}
import scala.concurrent.duration._

package object trips {
  implicit class DateTimeOps(ldt: LocalDateTime) {
    def getMillis: Long =
      ldt.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli

    def isBetween(start: LocalDateTime, end: LocalDateTime): Boolean =
      ldt.isAfter(start) && ldt.isBefore(end)

    def until(other: LocalDateTime): FiniteDuration = {
      (other.getMillis - ldt.getMillis).millis
    }

    def toUtilDate: java.util.Date =
      java.util.Date.from(Instant.ofEpochMilli(ldt.getMillis))

    def minus(duration: FiniteDuration): LocalDateTime =
      ldt.minusNanos(duration.toNanos)

    def plus(duration: FiniteDuration): LocalDateTime =
      ldt.plusNanos(duration.toNanos)
  }
  implicit class TimeOps(lt: LocalTime) {
    def isBetween(start: LocalTime, end: LocalTime): Boolean =
      lt.isAfter(start) && lt.isBefore(end)

    def until(other: LocalTime): FiniteDuration =
      java.time.Duration.between(lt, other).toMillis.millis
  }

  implicit class DateOps(ld: LocalDate) {
    def getMillis: Long = ld.atTime(LocalTime.MIN).getMillis

    def diff(other: LocalDate): FiniteDuration = {
      (ld.getMillis - other.getMillis).millis
    }

    def minus(duration: FiniteDuration): LocalDate =
      ld.minusDays(duration.toDays)
  }

  implicit val tripTypeEncoder: Encoder[TripType.Value] =
    Encoder.enumEncoder(TripType)
  implicit val tripTypeDecoder: Decoder[TripType.Value] =
    Decoder.enumDecoder(TripType)
}
