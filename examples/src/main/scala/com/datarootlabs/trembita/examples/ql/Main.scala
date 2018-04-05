package com.datarootlabs.trembita.examples.ql

import com.datarootlabs.trembita.ql._
import ArbitraryGroupResult._
import AggDecl._
import GroupingCriteria._
import instances._
import cats._
import cats.implicits._
import QueryBuilder._
import show._
import com.datarootlabs.trembita.ql.AggRes._


object Main extends algebra.instances.AllInstances {
  trait `divisible by 2`
  trait `divisible by 3`
  trait `reminder of 4`

  trait square
  trait count
  trait `^4`

  type Criterias =
    (Boolean ## `divisible by 2`) &::
      (Boolean ## `divisible by 3`) &::
      (Long ## `reminder of 4`) &::
      GNil

  type DividingAggDecl =
    TaggedAgg[Double, square, AggFunc.Type.Avg] %::
      TaggedAgg[Long, count, AggFunc.Type.Count] %::
      TaggedAgg[Long, `^4`, AggFunc.Type.Sum] %::
      DNil

  type DividingAggRes =
    (Double ## square) *::
      (Long ## count) *::
      (Long ## `^4`) *::
      RNil

  def main(args: Array[String]): Unit = {
    val numbers: List[Long] = (1L to 20L).toList
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
          (num * num * num * num).as[`^4`].sum %:: DNil
      )
      .having(_.get[count] > 7))

    println(result.pretty())
  }
}
