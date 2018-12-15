package com.examples.spark

import java.util.concurrent.Executors
import com.github.trembita._
import com.github.trembita.experimental.spark._
import org.apache.spark._
import cats.implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * To run this example, you need a spark-cluster.
  * Use docker-compose to deploy one
  *
  * @see resources/spark/cluster
  * */
object Main {
  implicit def ec: ExecutionContext = ExecutionContext.global
  val cahedThreadPool =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  def main(args: Array[String]): Unit = {
    implicit val sc: SparkContext =
      new SparkContext("spark://spark-master:7077", "trembita-spark")
    implicit val timeout: Timeout = Timeout(5.minutes)

    try {
      trembitaSample
    } finally {
      sc.stop()
    }
  }

  private def trembitaSample(implicit sc: SparkContext,
                             timeout: Timeout): Unit = {
    val numbers = DataPipelineT[Future, Int](1, 2, 3, 20, 40, 60)
      .to[Spark]
      .map(_ + 1)
      .mapM { i: Int =>
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
        } yield nx + bx).recover { case _ => -100500 }
      }

    println("TREMBITA EVAL")
    val result = Await.result(numbers.eval, Duration.Inf)
    println(result)
  }
}
