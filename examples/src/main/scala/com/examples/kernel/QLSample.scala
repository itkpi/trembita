package com.examples.kernel

import com.github.trembita.ql._
import com.examples.putStrLn
import com.github.trembita._
import com.github.trembita.ql.show._
import cats.implicits._
import cats.effect._
import shapeless._

object QLSample extends IOApp with algebra.instances.AllInstances {

  trait `divisible by 2`
  trait `divisible by 3`
  trait `reminder of 4`

  trait square
  trait count
  trait `^4`
  trait `some name`

  def run(args: List[String]): IO[ExitCode] = {
    val numbers =
      DataPipelineT.liftF[IO, Long, Sequential](IO { 1L to 20L })

    val result = numbers
      .to[Parallel]
      .query(
        _.filter(_ > 5)
          .groupBy(
            num =>
              (
                (num % 2 == 0).as[`divisible by 2`],
                (num % 3 == 0).as[`divisible by 3`],
                (num % 4).as[`reminder of 4`]
            )
          )
          .aggregate(
            num =>
              (
                (num * num).toDouble.as[square].avg,
                num.as[count].count,
                (num * num * num * num).as[`^4`].sum,
                num.toString.tagAs[`some name`].sum
            )
          )
          .having(_.get[count] > 7)
      )
      .eval
      .flatTap { result =>
        putStrLn("First one:") *>
          putStrLn(result.map(_.pretty()).mkString("\n")) *>
          putStrLn("-------------------------")
      }

    val numbersDP = DataPipelineT
      .liftF[IO, Long, Sequential](IO { 15L to 40L })
      .query(
        _.groupBy(
          num =>
            (
              (num % 2 == 0).as[`divisible by 2`],
              (num % 3 == 0).as[`divisible by 3`],
              (num % 4).as[`reminder of 4`]
          )
        ).aggregate(
            num =>
              (
                (num * num).toDouble.as[square].avg,
                num.as[count].count,
                (num * num * num * num).as[`^4`].sum,
                num.toString.tagAs[`some name`].sum
            )
          )
          .having(_.get[`some name`].contains('1'))
      )
      .as[NumbersReport] // transforms directly into case class
      .eval
      .flatTap { report =>
        putStrLn(s"Report: $report")
      }

    (result *> numbersDP).as(ExitCode.Success)
  }
}

case class Totals(square: Double, count: Long, power4: Long, someName: String)

case class NumbersReportReminderOf4(reminderOf4: Long,
                                    totals: Totals,
                                    values: List[Long])

case class NumbersReportDivisionBy3SubTotal(
  totals: Totals,
  subRecords: List[NumbersReportReminderOf4]
)

case class NumbersReportDivisionBy3(
  divisibleBy3: Boolean,
  totals: Totals,
  subRecords: List[NumbersReportDivisionBy3SubTotal]
)

case class NumbersReportDivisibleBy2SubTotal(
  totals: Totals,
  subRecords: List[NumbersReportDivisionBy3]
)

case class NumbersReportDivisionBy2(
  divisibleBy2: Boolean,
  totals: Totals,
  subRecords: List[NumbersReportDivisibleBy2SubTotal]
)

case class NumbersReport(totals: Totals,
                         subRecords: List[NumbersReportDivisionBy2])
