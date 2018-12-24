package com.github.trembita

import org.scalatest.FlatSpec
import ql._
import GroupingCriteria._
import AggRes._
import cats._
import shapeless.syntax.singleton._
import shapeless.Witness

class TrembitaQLSpec extends FlatSpec with algebra.instances.AllInstances {
  /* Only for testing */
  type `divisible by 2` = Witness.`"divisible by 2"`.T
  type sum              = Witness.`"sum"`.T
  type `reminder of 3`  = Witness.`"reminder of 3"`.T
  type positive         = Witness.`"positive"`.T
  type `all digits`     = Witness.`"all digits"`.T
  type `avg number`     = Witness.`"avg number"`.T
  type `max integer`    = Witness.`"max integer"`.T

  "A simple DataPipeline.query(...)" should "produce correct result" in {
    val pipeline = DataPipeline[Int](3, 1, 2)
    val result: Vector[QueryResult[Int, (Boolean :@ `divisible by 2`) &:: GNil, (Int :@ sum) *:: RNil]] =
      pipeline
        .query(
          _.groupBy(expr[Int](_ % 2 == 0) as "divisible by 2")
            .aggregate(col[Int] agg sum as "sum")
        )
        .eval

    assert(
      result == Vector(
        QueryResult(
          :@(false) &:: GNil,
          :@(4) *:: RNil,
          Vector(3, 1)
        ),
        QueryResult(
          :@(true) &:: GNil,
          :@(2) *:: RNil,
          Vector(2)
        )
      )
    )
  }

  "A simple DataPipeline.query(...) with ordering records" should "produce correct result" in {
    val pipeline = DataPipeline[Int](3, 1, 2)
    val result: Vector[QueryResult[Int, (Boolean :@ `divisible by 2`) &:: GNil, (Int :@ sum) *:: RNil]] =
      pipeline
        .query(
          _.groupBy(expr[Int](_ % 2 == 0) as "divisible by 2")
            .aggregate(col[Int] agg sum as "sum")
            .orderRecords
        )
        .eval

    assert(
      result == Vector(
        QueryResult(
          :@(false) &:: GNil,
          :@(4) *:: RNil,
          Vector(1, 3)
        ),
        QueryResult(
          :@(true) &:: GNil,
          :@(2) *:: RNil,
          Vector(2)
        )
      )
    )
  }

  "A complicated DataPipeline.query(...)" should "produce correct result" in {
    val pipeline = DataPipeline[Int](3, 1, 8, 2)
    val result: Vector[QueryResult[
      Int,
      (Boolean :@ `divisible by 2`) &:: (Int :@ `reminder of 3`) &:: (Boolean :@ positive) &:: GNil,
      (String :@ `all digits`) *:: (Double :@ `avg number`) *:: (Int :@ `max integer`) *:: RNil
    ]] =
      pipeline
        .query(
          _.groupBy(
            expr[Int](_ % 2 == 0) as "divisible by 2",
            expr[Int](_ % 3) as "reminder of 3",
            expr[Int](_ > 0) as "positive"
          ).aggregate(
              col[Int] agg stringAgg as "all digits",
              expr[Int](_.toDouble) agg avg as "avg number",
              col[Int] agg max as "max integer"
            )
            .ordered
        )
        .eval

    assert(
      result == Vector(
        QueryResult(
          :@(false) &:: :@(0) &:: :@(true) &:: GNil,
          :@("3") *:: :@(3.0) *:: :@(3) *:: RNil,
          Vector(3)
        ),
        QueryResult(
          :@(false) &:: :@(1) &:: :@(true) &:: GNil,
          :@("1") *:: :@(1.0) *:: :@(1) *:: RNil,
          Vector(1)
        ),
        QueryResult(
          :@(true) &:: :@(2) &:: :@(true) &:: GNil,
          :@("82") *:: :@(5.0) *:: :@(8) *:: RNil,
          Vector(2, 8)
        )
      )
    )
  }

  it should "be converted to case class correctly" in {
    case class Numbers(
        divisibleBy2: Boolean,
        reminderOf3: Int,
        positive: Boolean,
        allDigits: String,
        avg: Double,
        max: Int,
        values: Vector[Int]
    )

    val pipeline = DataPipeline[Int](3, 1, 8, 2)
    val result: Vector[Numbers] =
      pipeline
        .query(
          _.groupBy(
            expr[Int](_ % 2 == 0) as "divisible by 2",
            expr[Int](_ % 3) as "reminder of 3",
            expr[Int](_ > 0) as "positive"
          ).aggregate(
              col[Int] agg stringAgg as "all digits",
              expr[Int](_.toDouble) agg avg as "avg number",
              col[Int] agg max as "max integer"
            )
            .having(agg[Int]("max integer")(_ > 0))
            .ordered
        )
        .as[Numbers]
        .eval

    assert(
      result == Vector(
        Numbers(
          divisibleBy2 = false,
          reminderOf3 = 0,
          positive = true,
          allDigits = "3",
          avg = 3.0,
          max = 3,
          values = Vector(3)
        ),
        Numbers(false, 1, true, "1", 1.0, 1, Vector(1)),
        Numbers(true, 2, true, "82", 5.0, 8, Vector(2, 8))
      )
    )
  }
}
