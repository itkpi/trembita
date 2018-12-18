package com.github.trembita.ql

import scala.annotation.implicitNotFound
import scala.language.experimental.macros
import scala.language.higherKinds
import com.github.trembita._
import QueryBuilder._
import cats.Monad
import com.github.trembita.operations.CanSort
import scala.reflect.ClassTag

@implicitNotFound("""
    Aggregation upon ${A}
      - with grouping ${G}
      - aggregations ${T}
      - and expected result ${R}
    cannot be performed in ${Ex}.
    In most cases it means that ${Ex} does not support an efficient data querying
  """)
trait trembitaql[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb, Ex <: Environment] extends Serializable {
  def apply[F[_]](pipeline: DataPipelineT[F, A, Ex], queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb])(
      implicit F: Monad[F],
      ex: Ex,
      run: Ex#Run[F]
  ): DataPipelineT[F, QueryResult[A, G, R], Ex]
}

object trembitaql {
  def derive[A: ClassTag,
             G <: GroupingCriteria: ClassTag,
             T <: AggDecl: ClassTag,
             R <: AggRes: ClassTag,
             Comb: ClassTag,
             Ex <: Environment: ClassTag](
      implicit canSort: CanSort[Ex#Repr]
  ): trembitaql[A, G, T, R, Comb, Ex] =
    new trembitaql[A, G, T, R, Comb, Ex] {
      type QueryRes = QueryResult[A, G, R]
      override def apply[F[_]](
          pipeline: DataPipelineT[F, A, Ex],
          queryF: Empty[A] => Query[A, G, T, R, Comb]
      )(implicit F: Monad[F], ex: Ex, run: Ex#Run[F]): DataPipelineT[F, QueryResult[A, G, R], Ex] = {
        val query = queryF(new Empty[A])
        val grouped: DataPipelineT[F, QueryRes, Ex] = pipeline
          .filterImpl[A](query.filterF)
          .groupBy(query.getG)
          .mapValues { as =>
            val (asVector, totals) =
              as.foldLeft(Vector.empty[A], query.aggF.empty) {
                case ((accVs, comb), a) =>
                  (accVs :+ a, query.aggF.add(comb, query.getT(a)))
              }
            query.aggF.extract(totals) -> query.orderRecords.fold(asVector)(
              asVector.sorted(_)
            )
          }
          .mapImpl {
            case (g, (aggRes, vs)) => QueryResult(g, aggRes.result, vs)
          }
          .filterImpl(qr => query.havingF(qr.totals))

        val orderedByR: DataPipelineT[F, QueryRes, Ex] =
          query.orderResults.fold(grouped) { implicit orderingR =>
            grouped.mapRepr { repr =>
              val sorted = canSort.sortedBy[QueryRes, R](repr)(_.totals)
              sorted
            }
          }
        val orderedByG: DataPipelineT[F, QueryRes, Ex] =
          query.orderCriterias.fold(orderedByR) { implicit orderingG =>
            orderedByR.mapRepr { repr =>
              val sorted = canSort.sortedBy[QueryRes, G](repr)(_.keys)
              sorted
            }
          }
        orderedByG
      }
    }

  implicit def sequentialQL[A: ClassTag, G <: GroupingCriteria: ClassTag, T <: AggDecl: ClassTag, R <: AggRes: ClassTag, Comb: ClassTag](
      implicit canSort: CanSort[Sequential#Repr]
  ): trembitaql[A, G, T, R, Comb, Sequential] = derive

  implicit def parallelQL[A: ClassTag, G <: GroupingCriteria: ClassTag, T <: AggDecl: ClassTag, R <: AggRes: ClassTag, Comb: ClassTag](
      implicit canSort: CanSort[Parallel#Repr]
  ): trembitaql[A, G, T, R, Comb, Parallel] = derive
}
