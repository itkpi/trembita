package com.datarootlabs.trembita.examples.core


import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import cats.effect.implicits._
import com.datarootlabs.trembita._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try


object Main {
  private val flow = Kleisli[DataPipeline, String, String] { (lines: String) ⇒ DataPipeline.from(lines.split("\n")).par }
    .mapF(_.flatMap(_.split(" ")))
    .mapF(_.flatMap(numStr ⇒ Try(numStr.toInt).toOption))

  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[String] = DataPipeline(
      "1 2 3", "4 5 6", "7 8 9"
    )

    val numbers: DataPipeline[Int] = pipeline
      .flatMap(_.split(" "))
      .par
      .flatMap(numStr ⇒ Try(numStr.toInt).toOption)

    val sum: Int = numbers.foldLeft(0)(_ + _)
    println(s"Sum = $sum")

    val numbersIO = numbers.runM(Sync[IO])

    println(s"Nums: ${numbersIO.unsafeRunSync().mkString(", ")}")

    val numbers2 = pipeline.transform(flow).force.mkString(", ")
    println(s"Nums2: $numbers2")
  }
}
