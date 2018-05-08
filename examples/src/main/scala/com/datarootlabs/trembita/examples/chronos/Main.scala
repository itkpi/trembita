package com.datarootlabs.trembita.examples.chronos

import java.time._
import scala.concurrent.duration._
import com.datarootlabs.trembita.chronos._

object Main {
  def main(args: Array[String]): Unit = {
    val time: LocalTime = t"18:00:00"
    val date: LocalDate = d"2018-02-15"
    val date2: LocalDate = d"10.02.2018"
    val dateTime: LocalDateTime = dt"2018-02-15T18:00:00"
    val dateTime2: LocalDateTime = dt"15.02.2018 19:01:00"
    val dateTime3: LocalDateTime = dt"12.02.2018 10:00:00"
    val zoned: ZonedDateTime = zdt"2018-02-15T18:00:00+02:00[Europe/Kiev]"

    println(
      s"""
         time       : $time
         date       : $date
         date EU    : $date2
         dateTime   : $dateTime
         dateTime EU: $dateTime2
         zoned      : $zoned
         diff       : ${dateTime diff dateTime2}
         diff op    : ${dateTime -- dateTime2}
         plus       : ${dateTime plus 1.hour plus 30.minutes}
         minus      : ${dateTime minus 2.hours minus 10.minutes}
         plus op    : ${dateTime /+ 1.hour /+ 30.minutes}
         minus op   : ${dateTime /- 2.hours /- 18.minutes /- 20.seconds}
         is between : ${dateTime.isBetween(dateTime2, dateTime3)} & ${dateTime3.isBetween(dateTime, dateTime2)}
         isBetweenOp: ${dateTime <=<? (dateTime2, dateTime3)} & ${dateTime3 <=<? (dateTime, dateTime2)}
         to         : ${date2 to date}
         until      : ${date2 untilDate date}
       """)
  }
}
