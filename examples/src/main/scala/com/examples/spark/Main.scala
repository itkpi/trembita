package com.examples.spark

import java.util.concurrent.Executors
import cats.effect.Console.io._
import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all._
import com.github.trembita._
import com.github.trembita.spark.{Spark, _}
import org.apache.spark._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import com.github.trembita.logging._

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

        val numbers = Input
          .sequentialF[SerializableFuture, Seq]
          .create(SerializableFuture.pure(Seq(1, 2, 3, 20, 40, 60)))
          .to[Spark]
          // will be executed on spark
          .map(_ + 1)
          .mapM { i: Int =>
            val n = SerializableFuture.start { i + 1 }(cahedThreadPool)
            val b = SerializableFuture
              .start {
                val x = 1 + 2
                x * 3
              }
              .flatTap(
                xx =>
                  SerializableFuture.start {
                    println(s"spark debug: $xx") // you won't see this in submit logs
                }
              )

            val result: SerializableFuture[Int] =
              n.bind { nx =>
                b.where(nx > _).fmap(_ + nx)
              }

            result.attempt
          }
          .mapK(serializableFutureToIO)
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
            numbers
              .into(Output.vector)
              .run
              .flatMap(vs => putStrLn(vs.toString))
        }
      })(release = sc => IO { sc.stop() })
      .as(ExitCode.Success)
}
