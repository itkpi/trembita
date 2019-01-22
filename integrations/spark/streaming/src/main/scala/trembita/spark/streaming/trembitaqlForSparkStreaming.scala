package trembita.spark.streaming

import trembita.ql.QueryBuilder.Query
import trembita.ql.{AggDecl, AggRes, GroupingCriteria, QueryBuilder, QueryResult}
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

trait trembitaqlForSparkStreaming[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] extends Serializable {
  def apply(dstream: DStream[A], queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]): DStream[QueryResult[A, G, R]]
}

object trembitaqlForSparkStreaming {
  implicit def rddBased[A: ClassTag, G <: GroupingCriteria: ClassTag, T <: AggDecl, R <: AggRes, Comb]
    : trembitaqlForSparkStreaming[A, G, T, R, Comb] =
    new trembitaqlForSparkStreaming[A, G, T, R, Comb] {
      override def apply(
          rdd: DStream[A],
          queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]
      ): DStream[QueryResult[A, G, R]] = {

        val query: Query[A, G, T, R, Comb] = queryF(new QueryBuilder.Empty[A])

        val getG: A => G = query.getG
        val getT: A => T = query.getT
        val aggF         = query.aggF
        def orderedVs(vs: Vector[A]): Vector[A] =
          query.orderRecords.fold(vs)(vs.sorted(_))

        def sortAfterAgg(res: DStream[QueryResult[A, G, R]]): DStream[QueryResult[A, G, R]] = {
          val ordering: Option[Ordering[QueryResult[A, G, R]]] =
            query.orderCriterias.map { implicit orderG =>
              query.orderResults.fold[Ordering[QueryResult[A, G, R]]](ifEmpty = Ordering.by[QueryResult[A, G, R], G](_.keys)) {
                implicit orderR =>
                  Ordering.by[QueryResult[A, G, R], (G, R)](qr => (qr.keys, qr.totals))
              }
            } orElse {
              query.orderResults.map(implicit orderR => Ordering.by[QueryResult[A, G, R], R](_.totals))
            }

          ordering.fold(res)(implicit ord => res.transform(_.sortBy(q => q)))
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
            mergeCombiners _,
            partitioner = new HashPartitioner(12) // todo: make it more flexible
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
