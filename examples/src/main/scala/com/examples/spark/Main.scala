package com.examples.spark

import java.util.concurrent.Executors

import cats.effect.{ExitCode, IO, IOApp}
import com.github.trembita._
import com.github.trembita.experimental.spark._
import com.github.trembita.logging._
import org.apache.spark._
import cats.syntax.all._
import cats.effect.Console.io._
import com.github.trembita.spark.Spark

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * To run this example, you need a spark-cluster.
  * Use docker-compose to deploy one
  *
  * @see resources/spark/cluster
  * */
object Main extends IOApp {
  val cahedThreadPool =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  def run(args: List[String]): IO[ExitCode] =
    IO(new SparkContext("spark://spark-master:7077", "trembita-spark"))
      .bracket(use = { implicit trembitaSpark: SparkContext =>
        implicit val timeout: AsyncTimeout = AsyncTimeout(5.minutes)

        val numbers = DataPipelineT[Future, Int](1, 2, 3, 20, 40, 60)
          .to[Spark]
          // will be executed on spark
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
          .mapM { i =>
            IO { scala.util.Random.nextInt(10) + i }
          }
          // will be executed locally in parallel
          .to[Parallel]
          .info(i => s"parallel debug: $i") // you will see it in console
          .map(_ + 1)

        IO.suspend {
          putStrLn("TREMBITA eval") *>
            numbers.eval.flatTap(vs => putStrLn(vs.toString))
        }
      })(release = sc => IO { sc.stop() })
      .as(ExitCode.Success)
}
