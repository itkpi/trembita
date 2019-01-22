package com.examples.kernel

import cats.effect._
import cats.implicits._
import com.github.trembita._
import cats.effect.Console.io._
import scala.util.{Random, Success, Try}
import scala.util.control.NonFatal

object Basic extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val pipeline: DataPipelineT[IO, String, Sequential] =
      Input.sequentialF[IO, Seq].create(IO(Seq("1 2 3", "4 5 6", "7 8 9", "xyz")))

    val numbers: DataPipelineT[IO, Int, Parallel] = pipeline
      .to[Parallel]
      .mapConcat(_.split(" "))
      .map(_.toInt)
      .recoverNonFatal(_ => -100)

    val result1: IO[String] =
      numbers.into(Output.collection[Seq]).run.map(_.mkString(", ")).flatTap(putStrLn)

    val strings: DataPipelineT[IO, String, Parallel] = Input
      .randomF[IO]
      .create(RandomInput.propsT[IO, Int](n = 20, count = 10)(x => IO { x + 1 }))
      .map(_ + 1)
      .to[Parallel]
      .mapConcat(i => i :: (48 + i) :: Nil)
      .mapM { i =>
        putStrLn("mapM is working") *>
          IO { i * 12 }
      }
      .collect {
        case i if i % 2 == 0 => s"I'm an even number: $i"
      }
      .mapG(str => Try { str + "/Try" })
      .mapM(str => IO { str + "/IO" })

    val result2: IO[Vector[String]] = strings.into(Output.collection[Vector]).run

    result1.flatTap { result1 =>
      putStrLn(s"result1: $result1") *>
        putStrLn("------------------------------------")
    } *> result2.flatTap { result2 =>
      putStrLn(s"result2: $result2")
    } *>
      putStrLn("------------------------------------")
        .as(ExitCode.Success)
  }
}
