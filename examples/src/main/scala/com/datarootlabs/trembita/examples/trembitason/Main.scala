package com.datarootlabs.trembita.examples.trembitason

import com.datarootlabs.trembita.ql._
import com.datarootlabs.trembita.ql.show._
import com.datarootlabs.trembita.ql.instances._
import com.datarootlabs.trembitason.instances._
import io.circe._
import io.circe.syntax._
import cats._
import cats.implicits._
import ArbitraryGroupResult._
import GroupingCriteria._
import Aggregation._


object Main {
  trait MyNumber

  trait `divisible by 2`
  trait `reminder of 3`
  type Criteria = (Boolean ## `divisible by 2`) &:: (Int ## `reminder of 3`) &:: GNil

  trait square
  trait cube
  type Agg = (Int ## square) %:: (Int ## cube) %:: AgNil

  def main(args: Array[String]): Unit = {
    val a: Int ## MyNumber = 5.as[MyNumber]
    val aJson: Json = a.asJson
    println(aJson)

    val parsed = aJson.as[Int ## MyNumber]
    println(parsed.show)

    val criteria: Criteria = true.as[`divisible by 2`] &:: 1.as[`reminder of 3`] &:: GNil
    val crJson: Json = criteria.asJson
    println(crJson)

    val parsedCr = crJson.as[Criteria]
    println(parsedCr.show)

    val agg: Agg = 4.as[square] %:: 8.as[cube] %:: AgNil
    val agJson: Json = agg.asJson
    println(agJson)

    val parsedAgg = agJson.as[Agg]
    println(parsedAgg.show)

    val groupResult: ArbitraryGroupResult[Int, Criteria, Agg] = arbitraryGroupBy(1 to 5)(num ⇒
      (num % 2 == 0).as[`divisible by 2`] &:: (num % 3).as[`reminder of 3`] &:: GNil
    )(num ⇒
      (num * num).as[square] %:: (num * num * num).as[cube] %:: AgNil
    )

    val grResJson: Json = groupResult.asJson
    println(grResJson)

    val parsedGrRes = grResJson.as[ArbitraryGroupResult[Int, Criteria, Agg]]
    println(parsedGrRes.map(_.pretty()))
  }
}
