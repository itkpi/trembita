package com.datarootlabs.trembita

import org.scalatest.FlatSpec
import cats.data.NonEmptyList
import com.datarootlabs.trembita.ql._
import cats.implicits._
import AggDecl._
import AggRes._
import GroupingCriteria._
import QueryResult._
import shapeless._


class TrembitaQLSpec extends FlatSpec with algebra.instances.AllInstances {

  trait `divisible by 2`
  trait `reminder of 3`
  trait positive

  trait sum
  trait `all digits`
  trait `avg number`
  trait `max integer`


  "QueryResult Monoid" should "work correctly" in {
    val a: QueryResult[
      Int,
      (Boolean :@ `divisible by 2`) &:: GNil,
      AggFunc.Result[TaggedAgg[Int, sum, AggFunc.Type.Sum] %:: DNil, (Int :@ sum) *:: RNil, Int :: RNil :: HNil]
      ] = ~**(
      AggFunc.Result(6.as[sum] *:: RNil, 6 :: RNil :: HNil),
      ~::(
        Key.Single(true.as[`divisible by 2`]),
        AggFunc.Result(2.as[sum] *:: RNil, 2 :: RNil :: HNil),
        ##@(List(2))
      ),
      NonEmptyList(
        ~::(
          Key.Single(false.as[`divisible by 2`]),
          AggFunc.Result(4.as[sum] *:: RNil, 4 :: RNil :: HNil),
          ##@(List(1, 3))
        ),
        Nil
      )
    )
    val b = a |+| a
    assert(b == ~**(
      AggFunc.Result(12.as[sum] *:: RNil, 12 :: RNil :: HNil),
      ~::(
        Key.Single(true.as[`divisible by 2`]),
        AggFunc.Result(4.as[sum] *:: RNil, 4 :: RNil :: HNil),
        ##@(List(2, 2))
      ),
      NonEmptyList(
        ~::(
          Key.Single(false.as[`divisible by 2`]),
          AggFunc.Result(8.as[sum] *:: RNil, 8 :: RNil :: HNil),
          ##@(List(1, 3, 1, 3))
        ),
        Nil
      )
    ))

    val zero: QueryResult[
      Int,
      (Boolean :@ `divisible by 2`) &:: GNil,
      AggFunc.Result[TaggedAgg[Int, sum, AggFunc.Type.Sum] %:: DNil, (Int :@ sum) *:: RNil, Int :: RNil :: HNil]
      ] = QueryResult.Empty(AggFunc.Result(0.as[sum] *:: RNil, 0 :: RNil :: HNil))

    val c = a |+| zero
    assert(c == a)
  }

  "A simple List[Int].query(...)" should "produce correct result" in {
    val list: List[Int] = List(3, 1, 2)
    val result =
      list.query(_
        .groupBy(num ⇒ (num % 2 == 0).as[`divisible by 2`] &:: GNil)
        .aggregate(num ⇒ num.as[sum].sum %:: DNil)
      )

    assert(result == ~**(
      AggFunc.Result(6.as[sum] *:: RNil, 6 :: RNil :: HNil),
      ~::(
        Key.Single(true.as[`divisible by 2`]),
        AggFunc.Result(2.as[sum] *:: RNil, 2 :: RNil :: HNil),
        ##@(List(2))
      ),
      NonEmptyList(
        ~::(
          Key.Single(false.as[`divisible by 2`]),
          AggFunc.Result(4.as[sum] *:: RNil, 4 :: RNil :: HNil),
          ##@(List(3, 1))
        ),
        Nil
      )
    ))
  }

  "A simple List[Int].query(...) with ordering records" should "produce correct result" in {
    val list: List[Int] = List(3, 1, 2)
    val result =
      list.query(_
        .groupBy(num ⇒ (num % 2 == 0).as[`divisible by 2`] &:: GNil)
        .aggregate(num ⇒ num.as[sum].sum %:: DNil)
        .orderRecords
      )

    assert(result == ~**(
      AggFunc.Result(6.as[sum] *:: RNil, 6 :: RNil :: HNil),
      ~::(
        Key.Single(true.as[`divisible by 2`]),
        AggFunc.Result(2.as[sum] *:: RNil, 2 :: RNil :: HNil),
        ##@(List(2))
      ),
      NonEmptyList(
        ~::(
          Key.Single(false.as[`divisible by 2`]),
          AggFunc.Result(4.as[sum] *:: RNil, 4 :: RNil :: HNil),
          ##@(List(1, 3))
        ),
        Nil
      )
    ))
  }

  "A complicated List[Int].query(...)" should "produce correct result" in {
    val list: List[Int] = List(3, 1, 8, 2)
    val result =
      list.query(_
        .groupBy(num ⇒
          (num % 2 == 0).as[`divisible by 2`] &::
            (num % 3).as[`reminder of 3`] &::
            (num > 0).as[positive] &::
            GNil)
        .aggregate(num ⇒
          num.as[`all digits`].stringAgg %::
            num.toDouble.as[`avg number`].avg %::
            num.as[`max integer`].max %::
            DNil)
        .ordered
      )

    assert(result == ~**(
      AggFunc.Result(
        "8213".as[`all digits`] *:: 3.5.as[`avg number`] *:: 8.as[`max integer`] *:: RNil,
        new StringBuilder("8213") :: ((14.0 :: 4 :: HNil) :: (8 :: RNil :: HNil) :: HNil) :: HNil
      ),
      ~::(
        Key.Single(false.as[`divisible by 2`]),
        AggFunc.Result(
          "13".as[`all digits`] *:: 2.0.as[`avg number`] *:: 3.as[`max integer`] *:: RNil,
          new StringBuilder("13") :: ((4.0 :: 2 :: HNil) :: (3 :: RNil :: HNil) :: HNil) :: HNil
        ),
        ~**(
          AggFunc.Result(
            "13".as[`all digits`] *:: 2.0.as[`avg number`] *:: 3.as[`max integer`] *:: RNil,
            new StringBuilder("13") :: ((4.0 :: 2 :: HNil) :: (3 :: RNil :: HNil) :: HNil) :: HNil
          ),
          ~::(
            Key.Single(1.as[`reminder of 3`]),
            AggFunc.Result(
              "1".as[`all digits`] *:: 1.0.as[`avg number`] *:: 1.as[`max integer`] *:: RNil,
              new StringBuilder("1") :: ((1.0 :: 1 :: HNil) :: (1 :: RNil :: HNil) :: HNil) :: HNil
            ),
            ~::(
              Key.Single(true.as[positive]),
              AggFunc.Result(
                "1".as[`all digits`] *:: 1.0.as[`avg number`] *:: 1.as[`max integer`] *:: RNil,
                new StringBuilder("1") :: ((1.0 :: 1 :: HNil) :: (1 :: RNil :: HNil) :: HNil) :: HNil
              ),
              ##@(List(1))
            )
          ),
          NonEmptyList(
            ~::(
              Key.Single(0.as[`reminder of 3`]),
              AggFunc.Result(
                "3".as[`all digits`] *:: 3.0.as[`avg number`] *:: 3.as[`max integer`] *:: RNil,
                new StringBuilder("3") :: ((3.0 :: 1 :: HNil) :: (3 :: RNil :: HNil) :: HNil) :: HNil
              ),
              ~::(
                Key.Single(true.as[positive]),
                AggFunc.Result(
                  "3".as[`all digits`] *:: 3.0.as[`avg number`] *:: 3.as[`max integer`] *:: RNil,
                  new StringBuilder("3") :: ((3.0 :: 1 :: HNil) :: (3 :: RNil :: HNil) :: HNil) :: HNil
                ),
                ##@(List(3))
              )
            ),
            Nil
          )
        )
      ),
      NonEmptyList(
        ~::(
          Key.Single(true.as[`divisible by 2`]),
          AggFunc.Result(
            "82".as[`all digits`] *:: 5.0.as[`avg number`] *:: 8.as[`max integer`] *:: RNil,
            new StringBuilder("82") :: ((10.0 :: 2 :: HNil) :: (8 :: RNil :: HNil) :: HNil) :: HNil
          ),
          ~::(
            Key.Single(2.as[`reminder of 3`]),
            AggFunc.Result(
              "82".as[`all digits`] *:: 5.0.as[`avg number`] *:: 8.as[`max integer`] *:: RNil,
              new StringBuilder("82") :: ((10.0 :: 2 :: HNil) :: (8 :: RNil :: HNil) :: HNil) :: HNil
            ),
            ~::(
              Key.Single(true.as[positive]),
              AggFunc.Result(
                "82".as[`all digits`] *:: 5.0.as[`avg number`] *:: 8.as[`max integer`] *:: RNil,
                new StringBuilder("82") :: ((10.0 :: 2 :: HNil) :: (8 :: RNil :: HNil) :: HNil) :: HNil
              ),
              ##@(List(2, 8))
            )
          )
        ),
        Nil
      )
    ))
  }
}
