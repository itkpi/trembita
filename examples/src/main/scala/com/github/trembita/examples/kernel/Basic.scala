package com.github.trembita.examples.kernel

import cats.effect._
import cats.implicits._
import com.github.trembita._
import com.github.trembita.examples.putStrLn
import Execution._
import scala.util.{Random, Success, Try}
import scala.util.control.NonFatal

object Basic extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val pipeline: DataPipelineT[IO, String, Sequential] =
      DataPipelineT("1 2 3", "4 5 6", "7 8 9", "xyz")

    val numbers: DataPipelineT[IO, Int, Parallel] = pipeline
      .to[Parallel]
      .flatMap(_.split(" "))
      .map(_.toInt)
      .handleError { case NonFatal(_) => -100 }

    val result1: IO[String] =
      numbers.eval.map(_.mkString(", ")).flatTap(putStrLn)

    val strings: DataPipelineT[IO, String, Parallel] = DataPipelineT
      .randomInts[IO](20)
      .map(_ + 1)
      .to[Parallel]
      .flatMap(i => i :: (48 + i) :: Nil)
      .mapM { i =>
        putStrLn("mapM is working") *>
          IO { i * 12 }
      }
      .collect {
        case i if i % 2 == 0 => s"I'm an even number: $i"
      }
      .mapG(str => Try { str + "/Try" })
      .mapM(str => IO { str + "/IO" })

    val result2: Try[Vector[String]] = strings.mapK[Try].eval

    result1.flatTap { result1 =>
      putStrLn(s"result1: $result1") *>
        putStrLn("------------------------------------")
    } *> putStrLn(s"result2: $result2") *>
      putStrLn("------------------------------------")
        .as(ExitCode.Success)
  }
}
