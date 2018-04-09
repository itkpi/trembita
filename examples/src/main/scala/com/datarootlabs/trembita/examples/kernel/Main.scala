package com.datarootlabs.trembita.examples.kernel


import cats.data._
import cats.effect._
import cats.implicits._
import com.datarootlabs.trembita._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try


object Main {
  private val flow = Kleisli[DataPipeline, DataPipeline[String], String](_.par)
    .mapF(_.flatMap(_.split(" ")))
    .mapF(_.flatMap(numStr ⇒ Try(numStr.toInt).toOption))
    .mapF(_ :+ 2124)

  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[String] = DataPipeline(
      "1 2 3", "4 5 6", "7 8 9"
    )

    val numbers: DataPipeline[Int] = pipeline
      .flatMap(_.split(" "))
      .par
      .flatMap(numStr ⇒ Try(numStr.toInt).toOption)

    val nums2: DataPipeline[Int] = (DataPipeline("10 11 12", "13 11 15") :+ "48 7987 2")
      .par.transform(flow)
      .map(_ * 2)

    val result: IO[String] = (numbers ++ nums2).sorted.run[IO].map(_.mkString(", "))
    println(s"Result: ${result.unsafeRunSync()}")

    val sum: Int = numbers.foldLeft(0)(_ + _)
    println(s"Sum = $sum")
  }
}
