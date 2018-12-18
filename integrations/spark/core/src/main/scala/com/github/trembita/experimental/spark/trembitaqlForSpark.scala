package com.github.trembita.experimental.spark

import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Aggregation upon ${A}
      - with grouping ${G}
      - aggregations ${T}
      - and expected result ${R}
    cannot be performed in Spark.
    Please ensure implicit SparkSession in scope and ClassTag's for your data types
  """)
trait trembitaqlForSpark[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] extends Serializable {
  def apply(rdd: RDD[A], queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]): RDD[QueryResult[A, G, R]]
}
object trembitaqlForSpark {
  implicit def rddBased[A: ClassTag, G <: GroupingCriteria: ClassTag, T <: AggDecl, R <: AggRes, Comb](
      implicit spark: SparkSession
  ): trembitaqlForSpark[A, G, T, R, Comb] =
    new trembitaqlForSpark[A, G, T, R, Comb] {
      override def apply(
          rdd: RDD[A],
          queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]
      ): RDD[QueryResult[A, G, R]] = {

        val query: Query[A, G, T, R, Comb] = queryF(new QueryBuilder.Empty[A])

        val getG: A => G = query.getG
        val getT: A => T = query.getT
        val aggF         = query.aggF
        def orderedVs(vs: Vector[A]): Vector[A] =
          query.orderRecords.fold(vs)(vs.sorted(_))

        def sortAfterAgg(res: RDD[QueryResult[A, G, R]]): RDD[QueryResult[A, G, R]] = {
          val ordering: Option[Ordering[QueryResult[A, G, R]]] =
            query.orderCriterias.map { implicit orderG =>
              query.orderResults.fold[Ordering[QueryResult[A, G, R]]](ifEmpty = Ordering.by[QueryResult[A, G, R], G](_.keys)) {
                implicit orderR =>
                  Ordering.by[QueryResult[A, G, R], (G, R)](qr => (qr.keys, qr.totals))
              }
            } orElse {
              query.orderResults.map(implicit orderR => Ordering.by[QueryResult[A, G, R], R](_.totals))
            }

          ordering.fold(res)(implicit ord => res.sortBy(q => q))
        }

        def createCombiner(a: A): (Comb, Vector[A])                      = aggF.add(aggF.empty, getT(a)) -> Vector(a)
        def mergeValue(comb: (Comb, Vector[A]), a: A): (Comb, Vector[A]) = aggF.add(comb._1, getT(a))    -> (comb._2 :+ a)
        def mergeCombiners(c1: (Comb, Vector[A]), c2: (Comb, Vector[A])): (Comb, Vector[A]) =
          aggF.combine(c1._1, c2._1) -> (orderedVs(c1._2) ++ orderedVs(c2._2))

        val transformed = rdd
          .map(a => getG(a) -> a)
          .combineByKey[(Comb, Vector[A])](
            createCombiner _,
            mergeValue _,
            mergeCombiners _
          )
          .flatMap {
            case (group, (comb, vs)) =>
              val aggRes = aggF.extract(comb).result
              if (query.havingF(aggRes)) List(QueryResult[A, G, R](group, aggRes, orderedVs(vs)))
              else Nil
          }

        sortAfterAgg(transformed)
      }
    }
}
