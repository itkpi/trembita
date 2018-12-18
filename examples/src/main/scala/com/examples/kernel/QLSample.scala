package com.examples.kernel

import cats.implicits._
import cats.effect._
import com.github.trembita._
import com.github.trembita.ql._
import com.examples.putStrLn
import shapeless.syntax.singleton._

object QLSample extends IOApp with algebra.instances.AllInstances {
  def run(args: List[String]): IO[ExitCode] = {
    val numbers: DataPipelineT[IO, Long, Parallel] =
      DataPipelineT.liftF[IO, Long, Parallel](IO { 1L to 20L })

    val result = numbers
      .query(
        _.filter(_ > 5)
          .groupBy(
            expr[Int](_ % 2 == 0) as "divisible by 2",
            expr[Int](_ % 3 == 0) as "divisible by 3",
            expr[Int](_ % 4) as "reminder of 4"
          )
          .aggregate(
            expr[Int](num => (num * num).toDouble) agg avg as "square",
            col[Int] agg count as "count",
            expr[Int](num => num * num * num * num) agg sum as "^4",
            expr[Int](_.toString) agg sum as "some name"
          )
          .having(agg("count")(_ > 7))
      )
      .eval

    val numbersDP = DataPipelineT
      .liftF[IO, Long, Sequential](IO { 15L to 40L })
      .query(
        _.groupBy(
          expr[Int](_ % 2 == 0) as "divisible by 2",
          expr[Int](_ % 3 == 0) as "divisible by 3",
          expr[Int](_ % 4) as "reminder of 4"
        ).aggregate(
            expr[Int](num => (num * num).toDouble) agg avg as "square",
            col[Int] agg count as "count",
            expr[Int](num => num * num * num * num) agg sum as "^4",
            expr[Int](_.toString) agg sum as "some name"
          )
          .having(agg("some name")(_.contains('1')))
      )
      .as[NumbersReport] // transforms directly into case class
      .eval
      .flatTap { report =>
        putStrLn(s"Report: $report")
      }

    (result.flatTap { result =>
      putStrLn("First one:") *>
        putStrLn(result.mkString("\n")) *>
        putStrLn("-------------------------")
    } *> numbersDP).as(ExitCode.Success)
  }
}

case class NumbersReport(divisibleBy2: Boolean,
                         divisibleBy3: Boolean,
                         reminderOf4: Int,
                         square: Double,
                         count: Long,
                         power4: Long,
                         someName: String,
                         values: Vector[Int])
