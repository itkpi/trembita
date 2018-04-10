package com.datarootlabs.trembita.examples.ql

import com.datarootlabs.trembita.ql._
import AggDecl._
import AggRes._
import GroupingCriteria._
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.ql.show._
import cats.implicits._


object Main extends algebra.instances.AllInstances {
  trait `divisible by 2`
  trait `divisible by 3`
  trait `reminder of 4`

  trait square
  trait count
  trait `^4`
  trait `some name`

  def main(args: Array[String]): Unit = {
    val numbers: DataPipeline[Long] = DataPipeline.from(1L to 20L)
    val result = numbers.query(_
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
    println(result.map(_.pretty()).eval.mkString("\n---\n"))
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
    println(result2.map(_.pretty()).eval.mkString("\n---\n"))
    println("-------------------------")

    val sum = (result ++ result2).reduce
    println("\n Sum:")
    println(sum.pretty())
  }
}
