package com.github.trembita

import cats.effect._

package object examples {
  def putStrLn(a: Any): IO[Unit] = IO { println(a) }
}
