package com.datarootlabs.trembita.examples.ql

import com.datarootlabs.trembita.ql._
import AggDecl._
import AggRes._
import GroupingCriteria._
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.ql.show._
import cats.implicits._
import Finiteness.Finite
import Execution._

import scala.collection.parallel.immutable.ParVector
import scala.util.Try


object Main extends algebra.instances.AllInstances {

  trait `divisible by 2`
  trait `divisible by 3`
  trait `reminder of 4`

  trait square
  trait count
  trait `^4`
  trait `some name`

  def main(args: Array[String]): Unit = {
    val numbers: DataPipeline[Long, Try, Finite, Sequential] = DataPipeline.from(1L to 20L)
    val result = numbers
      .to[Parallel]
      .query(_
        .filter(_ > 5)
        .groupBy(num ⇒
          (num % 2 == 0).as[`divisible by 2`] &::
            (num % 3 == 0).as[`divisible by 3`] &::
            (num % 4).as[`reminder of 4`] &:: GNil
        )
        .aggregate(num ⇒
          (num * num).toDouble.as[square].avg %::
            num.as[count].count %::
            (num * num * num * num).as[`^4`].sum %::
            num.toString.as[`some name`].sum %:: DNil
        )
        .having(_.get[count] > 7))

    println("First one:")
    println(result.map(_.pretty()))
    println("-------------------------")

    val result2 = DataPipeline.from(15L to 40L).query(_
      .groupBy(num ⇒
        (num % 2 == 0).as[`divisible by 2`] &::
          (num % 3 == 0).as[`divisible by 3`] &::
          (num % 4).as[`reminder of 4`] &:: GNil
      )
      .aggregate(num ⇒
        (num * num).toDouble.as[square].avg %::
          num.as[count].count %::
          (num * num * num * num).as[`^4`].sum %::
          num.toString.as[`some name`].sum %::
          DNil
      )
      .having(_.get[`some name`].contains('1')))

    println("\nSecond:")
    println(result2.map(_.pretty()))
    println("-------------------------")

    val sum = result.get |+| result2.get
    println("\n Sum:")
    println(sum.pretty())
  }
}
