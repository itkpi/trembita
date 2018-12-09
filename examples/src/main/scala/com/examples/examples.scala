package com
import cats.effect.IO

package object examples {
  def putStrLn(a: Any): IO[Unit] = IO { println(a) }
}
