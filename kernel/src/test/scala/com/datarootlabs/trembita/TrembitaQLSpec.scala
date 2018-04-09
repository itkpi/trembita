package com.datarootlabs.trembita

import org.scalatest.FlatSpec
import cats.data.NonEmptyList
import com.datarootlabs.trembita.ql._
import AggDecl._
import AggRes._
import GroupingCriteria._
import ArbitraryGroupResult._


class TrembitaQLSpec extends FlatSpec with algebra.instances.AllInstances {

  trait `divisible by 2`
  trait sum

  "A simple List[Int].query(...)" should "produce correct result" in {
    val list: List[Int] = List(1, 2, 3)
    val result =
      list.query(_
        .groupBy(num ⇒ (num % 2 == 0).as[`divisible by 2`] &:: GNil)
        .aggregate(num ⇒ num.as[sum].sum %:: DNil)
      )

    assert(result == ~**(
      AggFunc.Result(6.as[sum] *:: RNil, (6, RNil)),
      ~::(
        Key.Single(true.as[`divisible by 2`]),
        AggFunc.Result(2.as[sum] *:: RNil, (2, RNil)),
        ##@(List(2))
      ),
      NonEmptyList(
        ~::(
          Key.Single(false.as[`divisible by 2`]),
          AggFunc.Result(4.as[sum] *:: RNil, (4, RNil)),
          ##@(List(1, 3))
        ),
        Nil
      )
    ))
  }
}
