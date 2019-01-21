package com.examples.spark.streaming

import java.util.concurrent.Executors

import cats.effect.{ExitCode, IO, IOApp}
import com.github.trembita._
import com.github.trembita.experimental.spark._
import com.github.trembita.experimental.spark.streaming._
import org.apache.spark._
import org.apache.spark.streaming.{StreamingContext, Duration => StreamingDuration}
import cats.syntax.all._
import cats.effect.Console.io._
import com.github.trembita.spark.streaming.SparkStreaming

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object Basic extends IOApp {
  val cahedThreadPool =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  def run(args: List[String]): IO[ExitCode] =
    IO(new SparkContext("spark://spark-master:7077", "trembita-spark"))
      .bracket(use = { implicit trembitaSpark: SparkContext =>
        implicit val ssc: StreamingContext = new StreamingContext(trembitaSpark, batchDuration = StreamingDuration(1000))
        implicit val timeout: AsyncTimeout = AsyncTimeout(5.minutes)

        val numbers: DataPipelineT[IO, Int, SparkStreaming] = DataPipelineT[Future, Int](1, 2, 3, 20, 40, 60)
          .to[SparkStreaming]
          .map(_ + 1)
          .mapM { i: Int =>
            val n = Future { i + 1 }(cahedThreadPool)
            val b = Future {
              val x = 1 + 2
              x * 3
            }.flatTap(
              xx =>
                Future {
                  println(s"spark debug: $xx") // you won't see this in submit logs
              }
            )

            (for {
              nx <- n
              bx <- b
              if nx > bx
            } yield nx + bx).attempt
          }
          .mapK(futureToIO)
          .map(_.getOrElse(-100500))

        numbers.evalRepr
          .map(_.print()) *> IO.delay {
          ssc.start()
          ssc.awaitTermination()
        }
      })(release = sc => IO { sc.stop() })
      .as(ExitCode.Success)
}
