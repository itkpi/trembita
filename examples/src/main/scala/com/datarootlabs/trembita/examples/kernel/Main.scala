package com.datarootlabs.trembita.examples.kernel


import java.time.LocalDateTime

import cats.data._
import cats.effect._
import cats.implicits._
import com.datarootlabs.trembita._
import PipelineType._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Random, Success, Try}
import scala.util.control.NonFatal
import scala.concurrent.{Await, Future}
import scala.io.StdIn
import scala.concurrent.duration._


object Main {
  //  private val flow: Flow[String, Int, IO, Finite] =
  //    Flow[String, String, IO, Finite](_.flatMap(_.split(" ")))
  //      .map(_.toInt)
  //      .mapF(_.handleError { case NonFatal(_) ⇒ -48 })

  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[String, Try, Finite] = DataPipeline.applyF(
      "1 2 3", "4 5 6", "7 8 9", "xyz"
    )

    val numbers: DataPipeline[Int, Try, Finite] = pipeline
      .flatMap(_.split(" "))
      .map(_.toInt)
      .handleError { case NonFatal(_) ⇒ -100 }

    //    val nums2: DataPipeline[Int, IO, Finite] = DataPipeline.applyF[String, IO]("10 11 12", "13 11 15")
    //      .transform(flow)
    //      .map(_ * 2)

    val result1: Try[String] = numbers.eval.map(_.mkString(", "))
    println(result1)

    val infinite: DataPipeline[Int, Future, Infinite] = DataPipeline.infinite { Random.nextInt() }

    val strings: DataPipeline[String, Future, Infinite] = infinite.map(_ + 1)
      .flatMap(i ⇒ i :: (48 + i) :: Nil)
      .mapM { i ⇒
        Future {
          println("mapM is working")
          i * 12
        }
      }
      .collect {
        case i if i % 2 == 0 ⇒ s"I'm an even number: $i"
      }
      .mapK[String, IO](str ⇒ IO { str + "/IO" })
      .mapM(str ⇒ Future { str + "/Future" })

    println("Haven't started...")


    val resF = strings.bind { res ⇒
      println(s"[${LocalDateTime.now}] result: $res")
      Future.unit
    }

    println("You can't see me!")
    //    val result2: IO[Int] = nums2.eval.map(_.sum)
    //    println(result2.unsafeRunSync())
    Await.result(resF, Duration.Inf)
  }
}
