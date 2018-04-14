package com.datarootlabs.trembita.examples.collections

import com.datarootlabs.trembita.collections.SizedAtLeast
import shapeless._
import shapeless.nat._
import cats.implicits._


object Main {
  def main(args: Array[String]): Unit = {
    val list: SizedAtLeast[Int, _5, Vector] = SizedAtLeast(_5)(1, 3, 7, 8, 7).to[Vector]
    println(list.show)
  }
}