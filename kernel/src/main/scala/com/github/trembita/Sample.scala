package com.github.trembita

import cats.effect.IO
import scala.language.higherKinds

object Sample extends App {
  val evaled: IO[(Vector[Int], Set[Int])] = DataPipelineT[IO, Sequential]
    .create(IO(Vector(1, 2, 3, 1, 2, 3)))
    .map(_ + 1)
    .into(foreach((x: Int) => println(s"first: $x + 1 = ${x + 1}")))
    .alsoInto(foreach((x: Int) => println(s"second: $x + 2 = ${x + 2}")))
    .ignoreBoth
    .alsoInto(foreach((x: Int) => println(s"third: $x + 3 = ${x + 2}")))
    .ignoreBoth
    .alsoInto(collection[Vector])
    .keepRight
    .alsoInto(collection[Set])
    .keepBothBind
    .run

  println(evaled.unsafeRunSync())
}
