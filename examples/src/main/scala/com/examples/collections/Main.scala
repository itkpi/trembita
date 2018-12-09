package com.examples.collections

import shapeless._
import shapeless.nat._
import cats.implicits._
import com.github.trembita.collections.SizedAtLeast


object Main {
  def main(args: Array[String]): Unit = {
    val list: SizedAtLeast[Int, _5, Vector] =
      SizedAtLeast(_5)(1, 3, 7, 5, 5, 5)[Vector]
    println(list.show)
  }
}
