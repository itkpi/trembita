package com.examples.spark

import java.util.concurrent.Executors

import com.github.trembita._
import com.github.trembita.experimental.spark._
import org.apache.spark._
import cats.syntax.all._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

/**
  * To run this example, you need a spark-cluster.
  * Use docker-compose to deploy one
  *
  * @see resources/spark/cluster
  * */
object Main {
  val cahedThreadPool =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  def main(args: Array[String]): Unit = {
    @transient implicit val trembitaSpark: SparkContext =
      new SparkContext("spark://spark-master:7077", "trembita-spark")
    implicit val timeout: Timeout = Timeout(5.minutes)

    val numbers = DataPipelineT[Future, Int](1, 2, 3, 20, 40, 60)
      .to[Spark]
      .map(_ + 1)
      .mapM { i: Int => // will be executed on spark
        val n = Future { i + 1 }(cahedThreadPool)
        val b = Future {
          val x = 1 + 2
          x * 3
        }.flatTap(
          xx =>
            Future {
              println(s"debug: $xx")
          }
        )

        (for {
          nx <- n
          bx <- b
          if nx > bx
        } yield nx + bx).attempt
      }
      .map(_.getOrElse(-100500))
      .to[Parallel]
      .map(_ + 1)

    try {
      println("TREMBITA EVAL")
      val result = Await.result(numbers.eval, Duration.Inf)
      println(result)
    } finally {
      trembitaSpark.stop()
    }
  }
}
