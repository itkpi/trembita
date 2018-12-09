package com.github.trembita.examples.ql

import com.github.trembita.ql._
import com.github.trembita.examples.putStrLn
import AggDecl._
import AggRes._
import GroupingCriteria._
import com.github.trembita._
import com.github.trembita.ql.show._
import cats.implicits._
import Execution._
import cats.effect._
import com.github.trembita.ql.AggFunc.Type

import scala.collection.parallel.immutable.ParVector
import scala.util.Try
import shapeless._
import shapeless.syntax.std.tuple._

object Main extends IOApp with algebra.instances.AllInstances {

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
                num.toString.as[`some name`].sum
            )
          )
          .having(_.get[count] > 7)
      )
      .flatTap { result =>
        putStrLn("First one:") *>
          putStrLn(result.pretty()) *>
          putStrLn("-------------------------")
      }

    val result2 = DataPipelineT
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
                num.toString.as[`some name`].sum
            )
          )
          .having(_.get[`some name`].contains('1'))
      )
      .flatTap { result2 =>
        putStrLn("\nSecond:") *>
          putStrLn(result2.pretty()) *>
          putStrLn("-------------------------")
      }

    case class Totals(square: Double,
                      count: Long,
                      power4: Long,
                      someName: String)

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

    val sum = for {
      res <- result
      res2 <- result2
    } yield res |+| res2

    sum
      .flatTap { sum =>
        putStrLn("\n Sum as case class:") *>
          putStrLn(sum.as[NumbersReport])
      }
      .as(ExitCode.Success)
  }
}