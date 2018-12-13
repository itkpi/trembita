package com.examples.spark

import com.github.trembita._
import com.github.trembita.experimental.spark._
import org.apache.spark._
import cats.instances.future._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

trait Resource {
  def use(): Unit
}

object Resource {
  def apply(): Resource = () => println("Used!")
}

object Main {

  def main(args: Array[String]): Unit = {
    implicit val sc: SparkContext = new SparkContext("local[*]", "trembita-spark")
    implicit val timeout: Timeout = Timeout(5.minutes)
    import ExecutionContext.Implicits.global

    val rdd = sc.parallelize(Seq(1, 2, 3))

    val numbers = DataPipelineT[Future, Int](1, 2, 3, 4, 5)
      .to[Spark]
      .map(_ + 1)
      .mapM { i: Int =>
        val n = i + 1
        Future {
          n * 100
        }
      }

    try {
      val result = Await.result(numbers.eval, Duration.Inf)
      println(result)
    } finally {
      sc.stop()
    }
  }
}
