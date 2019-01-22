package com.examples.spark.streaming

import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.sql._
import cats.implicits._
import com.examples.kernel.NumbersReport
import cats.effect.Console.io._
import trembita._
import trembita.ql._
import trembita.spark._
import trembita.spark.streaming._
import trembita.spark.streaming.SparkStreaming
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{StreamingContext, Duration => StreamingDuration}
import scala.concurrent.duration._
import shapeless.syntax.singleton._

object QLExample extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    IO(
      new SparkContext("spark://spark-master:7077", "trembita-spark")
    ).bracket(use = { trembitaSpark: SparkContext =>
        implicit val ssc: StreamingContext = new StreamingContext(trembitaSpark, batchDuration = StreamingDuration(1000))
        implicit val timeout: AsyncTimeout = AsyncTimeout(5.minutes)
        val numbers: DataPipelineT[IO, Long, SparkStreaming] =
          Input.liftF[IO, SparkStreaming].createF(IO { 1L to 2000L })

        val result = numbers
          .query(
            _.where(_ > 5)
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
              .having(agg[Long]("count")(_ > 7))
          )
          .as[NumbersReport]

        result
          .tapRepr(_.print())
          .into(Output.start)
          .run
          .flatMap(_ => IO { ssc.awaitTermination() })
      })(
        release = spark =>
          IO {
            spark.stop()
        }
      )
      .map(_ => ExitCode.Success)
}
