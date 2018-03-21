package com.datarootlabs.trembita.examples.core

import com.datarootlabs.trembita._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try


object Main {
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
  }
}
