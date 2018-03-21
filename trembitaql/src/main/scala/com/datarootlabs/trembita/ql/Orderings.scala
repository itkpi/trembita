package com.datarootlabs.trembita.ql

import java.time.{LocalDate, LocalDateTime, LocalTime}

trait Orderings {
  implicit val localDateOrdering    : Ordering[LocalDate]     = Ordering.fromLessThan(_ isBefore _)
  implicit val localTimeOrdering    : Ordering[LocalTime]     = Ordering.fromLessThan(_ isBefore _)
  implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.fromLessThan(_ isBefore _)
}