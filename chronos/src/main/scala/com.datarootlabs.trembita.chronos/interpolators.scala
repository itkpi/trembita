package com.datarootlabs.trembita.chronos


import java.time._
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import contextual._

import scala.util.Try


case object LocalTimeContext extends Context
object LocalTimeInterpolator extends Interpolator {

  type Output = LocalTime
  type ContextType = LocalTimeContext.type
  type Input = String

  def contextualize(interpolation: StaticInterpolation): Seq[ContextType] = {
    val lit@Literal(_, timeStr) = interpolation.parts.head
    if (Try {LocalTime.parse(timeStr)}.isFailure)
      interpolation.abort(lit, 0, "not a valid time")
    Nil
  }

  def evaluate(interpolation: RuntimeInterpolation): LocalTime =
    LocalTime.parse(interpolation.literals.head)
}

case object LocalDateContext extends Context
object LocalDateInterpolator extends Interpolator {

  type Output = LocalDate
  type ContextType = LocalDateContext.type
  type Input = String
  private val euFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")

  private def tryParse(dateStr: String): Try[LocalDate] = Try {LocalDate.parse(dateStr)}.orElse(Try {LocalDate.parse(dateStr, euFormatter)})

  def contextualize(interpolation: StaticInterpolation): Seq[ContextType] = {
    val lit@Literal(_, dateStr) = interpolation.parts.head
    if (tryParse(dateStr).isFailure)
      interpolation.abort(lit, 0, "not a valid date")
    Nil
  }

  def evaluate(interpolation: RuntimeInterpolation): LocalDate = tryParse(interpolation.literals.head).get
}

case object LocalDateTimeContext extends Context
object LocalDateTimeInterpolator extends Interpolator {

  type Output = LocalDateTime
  type ContextType = LocalTimeContext.type
  type Input = String
  private val euFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")

  private def tryParse(dateTimeStr: String): Try[LocalDateTime] = Try {LocalDateTime.parse(dateTimeStr)}.orElse(Try {LocalDateTime.parse(dateTimeStr, euFormatter)})

  def contextualize(interpolation: StaticInterpolation): Seq[ContextType] = {
    val lit@Literal(_, dateTimeStr) = interpolation.parts.head
    if (tryParse(dateTimeStr).isFailure)
      interpolation.abort(lit, 0, "not a valid date time")
    Nil
  }

  def evaluate(interpolation: RuntimeInterpolation): LocalDateTime =
    tryParse(interpolation.literals.head).get
}

case object ZonedDateTimeContext extends Context
object ZonedDateTimeInterpolator extends Interpolator {

  type Output = ZonedDateTime
  type ContextType = ZonedDateTimeContext.type
  type Input = String
  private val euFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss (XXX)")

  private def tryParse(dateTimeStr: String): Try[ZonedDateTime] = Try {ZonedDateTime.parse(dateTimeStr)}.orElse(Try {ZonedDateTime.parse(dateTimeStr, euFormatter)})

  def contextualize(interpolation: StaticInterpolation): Seq[ContextType] = {
    val lit@Literal(_, dateTimeStr) = interpolation.parts.head
    if (tryParse(dateTimeStr).isFailure)
      interpolation.abort(lit, 0, "not a valid zoned date time")
    Nil
  }

  def evaluate(interpolation: RuntimeInterpolation): ZonedDateTime =
    tryParse(interpolation.literals.head).get
}
