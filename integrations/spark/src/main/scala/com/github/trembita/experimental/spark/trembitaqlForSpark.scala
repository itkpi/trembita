package com.github.trembita.experimental.spark

import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox

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
//
//trait ToAggFunc[A] extends Serializable {
//  def apply(c: Column): Column
//}
//object ToAggFunc {
//  def apply[A](implicit ev: ToAggFunc[A]): ToAggFunc[A] = ev
//
//  implicit val sumFunc: ToAggFunc[AggFunc.Type.Sum]             = (c: Column) => functions.sum(c)
//  implicit val CountFunc: ToAggFunc[AggFunc.Type.Count]         = (c: Column) => functions.count(c)
//  implicit val AvgFunc: ToAggFunc[AggFunc.Type.Avg]             = (c: Column) => functions.avg(c)
//  implicit val MaxFunc: ToAggFunc[AggFunc.Type.Max]             = (c: Column) => functions.max(c)
//  implicit val MinFunc: ToAggFunc[AggFunc.Type.Min]             = (c: Column) => functions.min(c)
//  implicit val ProductFunc: ToAggFunc[AggFunc.Type.Product]     = (c: Column) => product(c)
//  implicit val RandomFunc: ToAggFunc[AggFunc.Type.Random]       = (c: Column) => random(c)
//  implicit val ArrFunc: ToAggFunc[AggFunc.Type.Arr]             = (c: Column) => functions.collect_list(c)
//  implicit val StringAggFunc: ToAggFunc[AggFunc.Type.StringAgg] = (c: Column) => functions.collect_list(c).cast(StringType)
//  implicit val STDEVFunc: ToAggFunc[AggFunc.Type.STDEV]         = (c: Column) => functions.stddev(c)
//  implicit val RMSFunc: ToAggFunc[AggFunc.Type.RMS]             = (c: Column) => rms(c)
//}
//
//trait ToColumn[A] extends Serializable {
//  def apply(): Column
//}
//object ToColumn {
//  def apply[A](implicit ev: ToColumn[A]): ToColumn[A] = ev
//
//  class toColumn(val c: blackbox.Context) {
//    import c.universe._
//
//    private val Col = typeOf[Column].dealias
//
//    private val sparkFunctionsImport =
//      q"import org.apache.spark.sql.functions._"
//
//    def taggetToColumnImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](
//        spark: c.Expr[SparkSession]
//    ): c.Expr[ToColumn[A :@ U]] = {
//      val A     = weakTypeOf[A].dealias
//      val U     = weakTypeOf[U]
//      val uName = U.toString
//
//      c.Expr[ToColumn[A :@ U]](q"""
//         new ToColumn[$A :@ $U] {
//           import $spark.implicits._
//           $sparkFunctionsImport
//           def apply(): $Col = col($uName).as[$A].alias($uName)
//         }
//       """)
//    }
//    def taggedAggToColumn[A: c.WeakTypeTag, U: c.WeakTypeTag, AggT <: AggFunc.Type: c.WeakTypeTag](
//        spark: c.Expr[SparkSession]
//    ): c.Expr[ToColumn[TaggedAgg[A, U, AggT]]] = {
//      val A    = weakTypeOf[A].dealias
//      val U    = weakTypeOf[U]
//      val AggT = weakTypeOf[AggT].dealias
//
//      val uName = U.toString
//
//      c.Expr[ToColumn[TaggedAgg[A, U, AggT]]](q"""
//           new ToColumn[TaggedAgg[$A, $U, $AggT]] {
//             import $spark.implicits._
//             $sparkFunctionsImport
//             private val aggToColumn = ToAggFunc[$AggT]
//             def apply(): Column = aggToColumn(col($uName)).alias($uName)
//           }
//         """)
//    }
//  }
//
//  implicit def taggedToColumn[A, U](
//      implicit spark: SparkSession
//  ): ToColumn[A :@ U] = macro toColumn.taggetToColumnImpl[A, U]
//
//  implicit def taggedAggToColumn[A, U, AggT <: AggFunc.Type](
//      implicit spark: SparkSession
//  ): ToColumn[TaggedAgg[A, U, AggT]] =
//    macro toColumn.taggedAggToColumn[A, U, AggT]
//}
//trait ToGroupByQuery[A] extends Serializable {
//  def apply(): List[Column]
//}
//object ToGroupByQuery {
//  def apply[A](implicit ev: ToGroupByQuery[A]): ToGroupByQuery[A] = ev
//
//  implicit val gnilCase: ToGroupByQuery[GroupingCriteria.GNil] =
//    new ToGroupByQuery[GroupingCriteria.GNil] {
//      override def apply(): List[Column] = Nil
//    }
//
//  implicit def consCase[H <: :@[_, _]: ToColumn, T <: GroupingCriteria: ToGroupByQuery]: ToGroupByQuery[H &:: T] =
//    new ToGroupByQuery[H &:: T] {
//      override def apply(): List[Column] =
//        ToColumn[H].apply() :: ToGroupByQuery[T].apply()
//    }
//}
//
//trait ToAggregateQuery[A] extends Serializable {
//  def apply(): List[Column]
//}
//object ToAggregateQuery {
//  def apply[A](implicit ev: ToAggregateQuery[A]): ToAggregateQuery[A] = ev
//
//  implicit val dnilCase: ToAggregateQuery[AggDecl.DNil] =
//    new ToAggregateQuery[AggDecl.DNil] {
//      override def apply(): List[Column] = Nil
//    }
//
//  implicit def consCase[H <: TaggedAgg[_, _, _]: ToColumn, T <: AggDecl: ToAggregateQuery]: ToAggregateQuery[H %:: T] =
//    new ToAggregateQuery[H %:: T] {
//      override def apply(): List[Column] =
//        ToColumn[H].apply() :: ToAggregateQuery[T].apply()
//    }
//}
//
//trait ToHavingFilter[A] extends Serializable {
//  def apply(): List[Column]
//}
//object ToHavingFilter {
//  def apply[A](implicit ev: ToHavingFilter[A]): ToHavingFilter[A] = ev
//  implicit val rnilCase: ToHavingFilter[AggRes.RNil] =
//    new ToHavingFilter[AggRes.RNil] {
//      def apply(): List[Column] = Nil
//    }
//  implicit def recCase[H <: :@[_, _]: ToColumn, T <: AggRes: ToHavingFilter]: ToHavingFilter[H *:: T] = new ToHavingFilter[H *:: T] {
//    def apply(): List[Column] = ToColumn[H].apply() :: ToHavingFilter[T].apply()
//  }
//}
//
//trait ToRow[A] extends Serializable {
//  def apply(a: A): Row
//  def schema: StructType
//}
//
//object ToRow {
//  def apply[A](implicit ev: ToRow[A]): ToRow[A] = ev
//
//  implicit def groupingHeadToRow[A, U](implicit A: Encoder[A]): ToRow[(A :@ U) &:: GNil] = new ToRow[(A :@ U) &:: GNil] {
//    def apply(a: (A :@ U) &:: GNil): Row = Row(a.head.value)
//    def schema: StructType               = A.schema
//  }
//
//  implicit def groupingRecToRow[A, U, G <: GroupingCriteria](implicit A: Encoder[A], G: ToRow[G], ev: G =:!= GNil): ToRow[(A :@ U) &:: G] =
//    new ToRow[(A :@ U) &:: G] {
//      def apply(a: (A :@ U) &:: G): Row = Row(Seq(a.head.value) ++ G(a.tail).toSeq: _*)
//      def schema: StructType            = StructType(A.schema.fields ++ G.schema.fields)
//    }
//
//  implicit def aggDeclHeadToRow[A, U, T <: AggFunc.Type](implicit A: Encoder[A]): ToRow[TaggedAgg[A, U, T] %:: DNil] =
//    new ToRow[TaggedAgg[A, U, T] %:: DNil] {
//      def apply(a: TaggedAgg[A, U, T] %:: DNil): Row = Row(a.head.tagged.value)
//      def schema: StructType                         = A.schema
//    }
//
//  implicit def aggDeclRecToRow[A, U, T <: AggFunc.Type, Agg <: AggDecl](implicit A: Encoder[A],
//                                                                        Agg: ToRow[Agg],
//                                                                        ev: Agg =:!= DNil): ToRow[TaggedAgg[A, U, T] %:: Agg] =
//    new ToRow[TaggedAgg[A, U, T] %:: Agg] {
//      def apply(a: TaggedAgg[A, U, T] %:: Agg): Row = Row(Seq(a.head.tagged.value) ++ Agg(a.tail).toSeq: _*)
//      def schema: StructType                        = StructType(A.schema.fields ++ Agg.schema.fields)
//    }
//}
//
//trait ToGroupByAgg[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] extends Serializable {
//  def apply(query: Query[A, G, T, R, Comb]): RDD[A] => DataFrame
//}
//object ToGroupByAgg {
//  implicit def derive[A: Encoder: ClassTag,
//                      G <: GroupingCriteria: ToGroupByQuery: ToRow: ClassTag,
//                      T <: AggDecl: ToAggregateQuery: ToRow: ClassTag,
//                      R <: AggRes: ToHavingFilter: ClassTag,
//                      Comb](implicit spark: SparkSession, ev: T =:!= DNil): ToGroupByAgg[A, G, T, R, Comb] =
//    new ToGroupByAgg[A, G, T, R, Comb] {
//      import spark.implicits._
//      import org.apache.spark.sql.functions.udf
//      override def apply(
//          query: Query[A, G, T, R, Comb]
//      ): RDD[A] => DataFrame = { rdd =>
//        val groupByQuery   = ToGroupByQuery[G].apply()
//        val aggregateQuery = ToAggregateQuery[T].apply()
//        val havingQuery    = ToHavingFilter[R].apply()
//
//        val groupToRow      = ToRow[G]
//        val aggDeclToRow    = ToRow[T]
//        val resultingSchema = StructType(implicitly[Encoder[A]].schema.fields ++ groupToRow.schema.fields ++ aggDeclToRow.schema.fields)
//
//        val getGRow = udf((a: A) => groupToRow(query.getG(a)))
//        val getTRow = udf((a: A) => aggDeclToRow(query.getT(a)))
//
////        val having          = udf(r: R => query.havingF(r))
////        val orderCriterias  = query.orderCriterias
////        val orderAggregates = query.orderResults
////        val orderRecords    = query.orderRecords
//
////        val orderings: List[Column] =
////          orderRecords.map(_ => col("_1")).toList ++
////            orderCriterias.map(_ => col("_2")) ++
////            orderAggregates.map(_ => col("_3"))
//
//        rdd
//          .filter(a => query.filterF(a))
//          .toDS()
//          .withColumn("value", $"*")
//          .withColumn("group", getGRow($"value"))
//          .withColumn("agg", getTRow($"value"))
//          .select($"value.*", $"group.*", $"agg.*")
//          .groupBy(groupByQuery: _*)
//          .agg(aggregateQuery.head, aggregateQuery.tail: _*)
//          .filterMany(havingQuery.head, havingQuery.tail: _*)
////          .orderBy(orderings: _*)
//      }
//    }
//}
