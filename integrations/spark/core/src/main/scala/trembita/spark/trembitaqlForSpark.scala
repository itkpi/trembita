package trembita.spark

import cats.Monad
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import trembita.BiDataPipelineT
import trembita.ql.QueryBuilder.Query
import trembita.ql._
import scala.language.higherKinds
import scala.reflect.ClassTag

trait trembitaqlForSpark {
  implicit def rddBased[A: ClassTag, G <: GroupingCriteria: ClassTag, T <: AggDecl, R <: AggRes, Comb](
      implicit spark: SparkSession
  ): trembitaql[A, G, T, R, Comb, Spark] =
    new trembitaql[A, G, T, R, Comb, Spark] {
      def apply[F[_]](query: Query[F, A, Spark, G, T, R, Comb])(
          implicit F: Monad[F],
          ex: Spark,
          run: RunOnSpark[F]
      ): BiDataPipelineT[F, QueryResult[A, G, R], Spark] = {

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

        query.pipeline
          .mapRepr { rdd =>
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
}
