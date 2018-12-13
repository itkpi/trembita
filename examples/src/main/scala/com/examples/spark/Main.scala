package com.examples.spark

import java.util.concurrent.Executors
import com.github.trembita._
import com.github.trembita.experimental.spark._
import org.apache.spark._
import cats.implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object Main {

  def main(args: Array[String]): Unit = {
    implicit val sc: SparkContext =
      new SparkContext("spark://spark-master:7077", "trembita-spark")
    implicit val timeout: Timeout = Timeout(5.minutes)
    implicit val ec: ExecutionContext = ExecutionContext.global

    val numbers = DataPipelineT[Future, Int](1, 2, 3, 20, 40, 60)
      .to[Spark]
      .map(_ + 1)
      .mapM { i: Int =>
        val n = Future { i + 1 }(ExecutionContext.fromExecutor(Executors.newCachedThreadPool()))
        val b = Future {
          val x = 1 + 2
          x * 3
        }.flatTap(xx => Future { println(s"debug: $xx") })

        (for {
          nx <- n
          bx <- b
          if nx > bx
        } yield nx + bx).recover { case _ => -100500 }
      }

//    val rdd = sc
//      .parallelize(Seq(1, 2, 3, 20, 40, 60))
//      .map { i: Int =>
//        val n = Future { i + 1 }
//        val b = Future {
//          val x = 1 + 2
//          x * 3
//        }.flatTap(xx => Future { println(s"debug: $xx") })
//
//        val result = (for {
//          nx <- n
//          bx <- b
//          if nx > bx
//        } yield nx + bx).recover { case _ => -100500 }
//        Await.result(result, timeout.duration)
//      }

    try {
      val result = args.headOption.map(_.toLowerCase) match {
//        case Some("rdd") =>
//          println("RDD COLLECT")
//          rdd.collect().toVector
        case _           =>
          println("TREMBITA EVAL")
          Await.result(numbers.eval, Duration.Inf)
      }
      println(result)
    } finally {
      sc.stop()
    }
  }
//  val x = main
}
