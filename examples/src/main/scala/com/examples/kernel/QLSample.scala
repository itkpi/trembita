package com.examples.kernel

import cats.implicits._
import cats.effect._
import trembita._
import trembita.ql._
import cats.effect.Console.io._
import shapeless.syntax.singleton._

object QLSample extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val numbers: BiDataPipelineT[IO, Long, Parallel] =
      Input.parallelF[IO, Seq].create(IO { 1L to 20L })

    val result = numbers
      .where(_ > 5)
      .groupBy(
        expr[Long](_ % 2 == 0) as "divisible by 2",
        expr[Long](_ % 3 == 0) as "divisible by 3",
        expr[Long](_ % 4) as "reminder of 4"
      )
      .aggregate(
        expr[Long](num => (num * num).toDouble) agg avg as "square",
        col[Long] agg count as "count",
        expr[Long](num => num * num * num * num) agg sum as "^4",
        expr[Long](_.toString) agg sum as "some name"
      )
      .compile
      .into(Output.vector)
      .run

    val numbersDP = Input
      .parallelF[IO, Seq]
      .create(IO { 15L to 40L })
      .groupBy(
        expr[Long](_ % 2 == 0) as "divisible by 2",
        expr[Long](_ % 3 == 0) as "divisible by 3",
        expr[Long](_ % 4) as "reminder of 4",
        expr[Long](_.toString.length) as "length"
      )
      .aggregate(
        expr[Long](num => (num * num).toDouble) agg avg as "square",
        col[Long] agg count as "count",
        expr[Long](num => num * num * num * num) agg sum as "^4",
        expr[Long](_.toString) agg sum as "some name",
        expr[Long](_.toDouble) agg stdev as "STDEV"
      )
      .having(agg[String]("some name")(_.contains('1')))
      .compile
      .as[NumbersReport] // transforms directly into case class
      .into(Output.vector)
      .run
      .flatMap { report =>
        putStrLn(s"Report: $report")
      }

    (result.flatMap { result =>
      putStrLn("First one:") *>
        putStrLn(result.mkString("\n")) *>
        putStrLn("-------------------------")
    } *> numbersDP).as(ExitCode.Success)
  }
}

case class NumbersReport(divisibleBy2: Boolean,
                         divisibleBy3: Boolean,
                         reminderOf4: Long,
                         length: Int,
                         square: Double,
                         count: Long,
                         power4: Long,
                         someName: String,
                         stdev: Double,
                         values: Vector[Long])
