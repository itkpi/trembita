package com.github.trembita

import cats.effect.IO
import scala.language.higherKinds

object Sample extends App {
  val pipeline: DataPipelineT[IO, Int, Parallel] = Input
    .parallelF[IO, Seq]
    .create[Int](IO(1 to 100))

  val (setIO, (strIO, sizeIO)): (IO[Set[Int]], (IO[String], IO[Int])) = pipeline
    .map(_ + 1)
    .into(Output.foreach((x: Int) => println(s"first: $x + 1 = ${x + 1}")))
    .alsoInto(Output.foreach((x: Int) => println(s"second: $x + 2 = ${x + 2}")))
    .ignoreBoth
    .alsoInto(Output.foreach((x: Int) => println(s"third: $x + 3 = ${x + 2}")))
    .ignoreBoth
    .alsoInto(Output.foldLeft[Int, String](zero = "") {
      case ("", i)  => i.toString
      case (acc, i) => s"$acc & $i"
    })
    .keepRight
    .alsoInto(Output.size)
    .keepBoth
    .alsoInto(Output.collection[Set])
    .keepBoth
    .run

  val result = for {
    str  <- strIO
    set  <- setIO
    size <- sizeIO
  } yield s"size: $size\nstr=$str\nset=$set"

  println(result.unsafeRunSync())
}
