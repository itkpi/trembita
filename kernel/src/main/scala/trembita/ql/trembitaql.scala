package trembita.ql

import scala.annotation.implicitNotFound
import scala.language.experimental.macros
import scala.language.higherKinds
import trembita._
import QueryBuilder._
import cats.Monad
import trembita.operations.{CanGroupBy, CanSort}

import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag

@implicitNotFound("""
    Aggregation upon ${A}
      - with grouping ${G}
      - aggregations ${T}
      - and expected result ${R}
    cannot be performed in ${E}.
    In most cases it means that ${E} does not support an efficient data querying
  """)
trait trembitaql[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb, E <: Environment] extends Serializable {
  def apply[F[_]](query: Query[F, A, E, G, T, R, Comb])(
      implicit F: Monad[F],
      ex: E,
      run: E#Run[F]
  ): DataPipelineT[F, QueryResult[A, G, R], E]
}

object trembitaql {
  def derive[A: ClassTag,
             G <: GroupingCriteria: ClassTag,
             T <: AggDecl: ClassTag,
             R <: AggRes: ClassTag,
             Comb: ClassTag,
             E <: Environment: ClassTag](
      implicit canSort: CanSort[E#Repr],
      canGroupBy: CanGroupBy[E#Repr]
  ): trembitaql[A, G, T, R, Comb, E] =
    new trembitaql[A, G, T, R, Comb, E] {
      type QueryRes = QueryResult[A, G, R]
      override def apply[F[_]](
          query: Query[F, A, E, G, T, R, Comb]
      )(implicit F: Monad[F], ex: E, run: E#Run[F]): DataPipelineT[F, QueryResult[A, G, R], E] = {
        val grouped: DataPipelineT[F, QueryRes, E] = query.pipeline
          .filterImpl[A](query.filterF)
          .groupByKey(query.getG)
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

        val orderedByR: DataPipelineT[F, QueryRes, E] =
          query.orderResults.fold(grouped) { implicit orderingR =>
            grouped.mapRepr { repr =>
              val sorted = canSort.sortedBy[QueryRes, R](repr)(_.totals)
              sorted
            }
          }
        val orderedByG: DataPipelineT[F, QueryRes, E] =
          query.orderCriterias.fold(orderedByR) { implicit orderingG =>
            orderedByR.mapRepr { repr =>
              val sorted = canSort.sortedBy[QueryRes, G](repr)(_.keys)
              sorted
            }
          }
        orderedByG
      }
    }

  implicit def sequentialQL[
      A: ClassTag,
      G <: GroupingCriteria: ClassTag,
      T <: AggDecl: ClassTag,
      R <: AggRes: ClassTag,
      Comb: ClassTag
  ](
      implicit canSort: CanSort[Sequential#Repr],
      canGroupBy: CanGroupBy[Sequential#Repr]
  ): trembitaql[A, G, T, R, Comb, Sequential] = derive

  implicit def parallelQL[
      A: ClassTag,
      G <: GroupingCriteria: ClassTag,
      T <: AggDecl: ClassTag,
      R <: AggRes: ClassTag,
      Comb: ClassTag,
      Implicits[_, _]
  ](
      implicit canSort: CanSort[Parallel#Repr],
      canGroupBy: CanGroupBy[Parallel#Repr]
  ): trembitaql[A, G, T, R, Comb, Parallel] = derive
}
