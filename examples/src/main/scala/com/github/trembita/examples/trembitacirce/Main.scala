package com.github.trembita.examples.trembitacirce

import com.github.trembita.ql._
import com.github.trembita.ql.show._
import com.github.trembita.circe._
import io.circe._
import io.circe.syntax._
import cats._
import cats.implicits._
import spire.implicits._
import QueryResult._
import GroupingCriteria._
import AggRes._
import AggDecl._
import com.github.trembita.ql.AggFunc.Type
import shapeless._

object Main {
  trait MyNumber

  trait `divisible by 2`
  trait `reminder of 3`
  type Criteria =
    (Boolean :@ `divisible by 2`) &:: (Int :@ `reminder of 3`) &:: GNil

  trait `sum of squares`
  trait `standard deviation`
  type AggDecl =
    TaggedAgg[Int, `sum of squares`, AggFunc.Type.Sum] %::
      TaggedAgg[Int, `standard deviation`, AggFunc.Type.STDEV] %:: DNil

  type Agg =
    (Int :@ `sum of squares`) *:: (Int :@ `standard deviation`) *:: RNil

  /** Thank You, Intellij! */
  type QRes =
    QueryResult[Int, &::[:@[Boolean, `divisible by 2`],
                         &::[:@[Int, `reminder of 3`], GNil]], AggFunc.Result[
      %::[TaggedAgg[Int, `sum of squares`, Type.Sum],
          %::[TaggedAgg[Double, `standard deviation`, Type.STDEV], DNil]],
      *::[:@[Int, `sum of squares`], *::[:@[Double, `standard deviation`],
                                         RNil]],
      ::[Int, ::[::[::[Double, ::[Vector[Double], ::[BigInt, HNil]]],
                    ::[RNil, HNil]], HNil]]
    ]]

  def main(args: Array[String]): Unit = {
    val a: Int :@ MyNumber = 5.as[MyNumber]
    val aJson: Json = a.asJson
    println(aJson)

    val parsed = aJson.as[Int :@ MyNumber]
    println(parsed.show)

    val criteria: Criteria = true
      .as[`divisible by 2`] &:: 1.as[`reminder of 3`] &:: GNil
    val crJson: Json = criteria.asJson
    println(crJson)

    val parsedCr = crJson.as[Criteria]
    println(parsedCr.show)

    val agg: Agg = 4
      .as[`sum of squares`] *:: 8.as[`standard deviation`] *:: RNil
    val agJson: Json = agg.asJson
    println(agJson)

    val parsedAgg = agJson.as[Agg]
    println(parsedAgg.show)

    val groupResult: QRes =
      (-5 to 20).query(
        _.groupBy(
          num =>
            (num % 2 == 0)
              .as[`divisible by 2`] &:: (num % 3).as[`reminder of 3`] &:: GNil
        ).aggregate(
            num =>
              (num * num).as[`sum of squares`].sum %:: num.toDouble
                .as[`standard deviation`]
                .deviation %:: DNil
          )
          .having(_.get[`sum of squares`] >= 50)
          .ordered
      )

    val grResJson: Json = groupResult.asJson
    println(grResJson)

    val parsedGrRes = grResJson.as[QRes]
    println(parsedGrRes.map(_.pretty()))
  }
}
