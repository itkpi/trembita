package com.datarootlabs.trembita.examples.ql

import com.datarootlabs.trembita.ql._
import ArbitraryGroupResult._
import Aggregation._
import GroupingCriteria._
import instances._
import cats._
import cats.implicits._
import show._


object Main {
  trait `divisible by 2`
  trait `divisible by 3`
  trait `reminder of 4`

  trait square
  trait cube
  trait `^4`

  type Criterias =
    (Boolean ## `divisible by 2`) &::
      (Boolean ## `divisible by 3`) &::
      (Long ## `reminder of 4`) &::
      GNil

  type DividingAggs =
    (Long ## square) %::
      (Long ## cube) %::
      (Long ## `^4`) %::
      AgNil

  def main(args: Array[String]): Unit = {
    val numbers: Seq[Long] = 1L to 8L

    val result: ArbitraryGroupResult[Long, Criterias, DividingAggs] = arbitraryGroupBy(numbers)(num ⇒
      (num % 2 == 0).as[`divisible by 2`] &:: (num % 3 == 0).as[`divisible by 3`] &:: (num % 4).as[`reminder of 4`] &:: GNil
    )(num ⇒
      (num * num).as[square] %:: (num * num * num).as[cube] %:: (num * num * num * num).as[`^4`] %:: AgNil
    )

    println(result.pretty())
  }
}
