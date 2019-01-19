package com.github.trembita

import cats.effect.IO
import scala.language.higherKinds

object Sample extends App {
  val ((vecIO, setIO), listIO) = DataPipelineT[IO, Parallel]
    .create(IO(1 to 100))
    .map(_ + 1)
    .into(Output.foreach((x: Int) => println(s"first: $x + 1 = ${x + 1}")))
    .alsoInto(Output.foreach((x: Int) => println(s"second: $x + 2 = ${x + 2}")))
    .ignoreBoth
    .alsoInto(Output.foreach((x: Int) => println(s"third: $x + 3 = ${x + 2}")))
    .ignoreBoth
    .alsoInto(Output.collection[Vector])
    .keepRight
    .alsoInto(Output.collection[Set])
    .keepBoth
    .alsoInto(Output.collection[List])
    .keepBoth
    .run

  val result = for {
    vec  <- vecIO
    set  <- setIO
    list <- listIO
  } yield s"vector=$vec, set=$set, list=$list"

  println(result.unsafeRunSync())
}
