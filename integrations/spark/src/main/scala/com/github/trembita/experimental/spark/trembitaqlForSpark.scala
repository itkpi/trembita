package com.github.trembita.experimental.spark

import com.github.trembita._
import com.github.trembita.ql.AggDecl.{%::, DNil}
import com.github.trembita.ql.AggRes.*::
import com.github.trembita.ql.GroupingCriteria.&::
import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql.{col => _, stdev => _, rms => _, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import shapeless.=:!=
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox

abstract class trembitaqlForSpark[A: Encoder, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] {
  def apply(rdd: RDD[A], queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]): RDD[Row]
}
object trembitaqlForSpark {
  implicit def derive[A: Encoder, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
      implicit spark: SparkSession,
      ev: ToGroupByAgg[A, G, T, R, Comb]
  ): trembitaqlForSpark[A, G, T, R, Comb] =
    new trembitaqlForSpark[A, G, T, R, Comb] {
      override def apply(
          rdd: RDD[A],
          queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]
      ): RDD[Row] = {
        import spark.implicits._

        val dataset: Dataset[A] = rdd.toDS()

        val query: Query[A, G, T, R, Comb] = queryF(new QueryBuilder.Empty[A])
        val result: DataFrame              = ev(query)(dataset)
        result.rdd
      }
    }
}

trait ToAggFunc[A] {
  def apply(c: Column): Column
}
object ToAggFunc {
  def apply[A](implicit ev: ToAggFunc[A]): ToAggFunc[A] = ev

  implicit val sumFunc: ToAggFunc[AggFunc.Type.Sum]             = (c: Column) => functions.sum(c)
  implicit val CountFunc: ToAggFunc[AggFunc.Type.Count]         = (c: Column) => functions.count(c)
  implicit val AvgFunc: ToAggFunc[AggFunc.Type.Avg]             = (c: Column) => functions.avg(c)
  implicit val MaxFunc: ToAggFunc[AggFunc.Type.Max]             = (c: Column) => functions.max(c)
  implicit val MinFunc: ToAggFunc[AggFunc.Type.Min]             = (c: Column) => functions.min(c)
  implicit val ProductFunc: ToAggFunc[AggFunc.Type.Product]     = (c: Column) => product(c)
  implicit val RandomFunc: ToAggFunc[AggFunc.Type.Random]       = (c: Column) => random(c)
  implicit val ArrFunc: ToAggFunc[AggFunc.Type.Arr]             = (c: Column) => functions.collect_list(c)
  implicit val StringAggFunc: ToAggFunc[AggFunc.Type.StringAgg] = (c: Column) => functions.collect_list(c).cast(StringType)
  implicit val STDEVFunc: ToAggFunc[AggFunc.Type.STDEV]         = (c: Column) => functions.stddev(c)
  implicit val RMSFunc: ToAggFunc[AggFunc.Type.RMS]             = (c: Column) => rms(c)
}

trait ToColumn[A] {
  def apply(): Column
}
object ToColumn {
  def apply[A](implicit ev: ToColumn[A]): ToColumn[A] = ev

  class toColumn(val c: blackbox.Context) {
    import c.universe._

    private val Col = typeOf[Column].dealias

    private val sparkFunctionsImport =
      q"import org.apache.spark.sql.functions._"

    def taggetToColumnImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](
        spark: c.Expr[SparkSession]
    ): c.Expr[ToColumn[A :@ U]] = {
      val A     = weakTypeOf[A].dealias
      val U     = weakTypeOf[U]
      val uName = U.toString

      c.Expr[ToColumn[A :@ U]](q"""
         new ToColumn[$A :@ $U] {
           import $spark.implicits._
           $sparkFunctionsImport
           def apply(): $Col = col("_2." + $uName).as[$A]
         }
       """)
    }
    def taggedAggToColumn[A: c.WeakTypeTag, U: c.WeakTypeTag, AggT <: AggFunc.Type: c.WeakTypeTag](
        spark: c.Expr[SparkSession]
    ): c.Expr[ToColumn[TaggedAgg[A, U, AggT]]] = {
      val A    = weakTypeOf[A].dealias
      val U    = weakTypeOf[U]
      val AggT = weakTypeOf[AggT].dealias

      val uName = U.toString

      c.Expr[ToColumn[TaggedAgg[A, U, AggT]]](q"""
           new ToColumn[TaggedAgg[$A, $U, $AggT]] {
             import $spark.implicits._
             $sparkFunctionsImport
             private val aggToColumn = ToAggFunc[$AggT]
             def apply(): Column = aggToColumn(col("_3")).alias($uName)
           }
         """)
    }
  }

  implicit def taggedToColumn[A, U](
      implicit spark: SparkSession
  ): ToColumn[A :@ U] = macro toColumn.taggetToColumnImpl[A, U]

  implicit def taggedAggToColumn[A, U, AggT <: AggFunc.Type](
      implicit spark: SparkSession
  ): ToColumn[TaggedAgg[A, U, AggT]] =
    macro toColumn.taggedAggToColumn[A, U, AggT]
}
trait ToGroupByQuery[A] {
  def apply(): List[Column]
}
object ToGroupByQuery {
  def apply[A](implicit ev: ToGroupByQuery[A]): ToGroupByQuery[A] = ev

  implicit val gnilCase: ToGroupByQuery[GroupingCriteria.GNil] =
    new ToGroupByQuery[GroupingCriteria.GNil] {
      override def apply(): List[Column] = Nil
    }

  implicit def consCase[H <: :@[_, _]: ToColumn, T <: GroupingCriteria: ToGroupByQuery]: ToGroupByQuery[H &:: T] =
    new ToGroupByQuery[H &:: T] {
      override def apply(): List[Column] =
        ToColumn[H].apply() :: ToGroupByQuery[T].apply()
    }
}

trait ToAggregateQuery[A] {
  def apply(): List[Column]
}
object ToAggregateQuery {
  def apply[A](implicit ev: ToAggregateQuery[A]): ToAggregateQuery[A] = ev

  implicit val dnilCase: ToAggregateQuery[AggDecl.DNil] =
    new ToAggregateQuery[AggDecl.DNil] {
      override def apply(): List[Column] = Nil
    }

  implicit def consCase[H <: TaggedAgg[_, _, _]: ToColumn, T <: AggDecl: ToAggregateQuery]: ToAggregateQuery[H %:: T] =
    new ToAggregateQuery[H %:: T] {
      override def apply(): List[Column] =
        ToColumn[H].apply() :: ToAggregateQuery[T].apply()
    }
}

trait ToHavingFilter[A] {
  def apply(): List[Column]
}
object ToHavingFilter {
  def apply[A](implicit ev: ToHavingFilter[A]): ToHavingFilter[A] = ev
  implicit val rnilCase: ToHavingFilter[AggRes.RNil] =
    new ToHavingFilter[AggRes.RNil] {
      def apply(): List[Column] = Nil
    }
  implicit def recCase[H <: :@[_, _]: ToColumn, T <: AggRes: ToHavingFilter]: ToHavingFilter[H *:: T] = new ToHavingFilter[H *:: T] {
    def apply(): List[Column] = ToColumn[H].apply() :: ToHavingFilter[T].apply()
  }
}

trait ToGroupByAgg[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] {
  def apply(query: Query[A, G, T, R, Comb]): Dataset[A] => DataFrame
}
object ToGroupByAgg {
  implicit def derive[A: Encoder: ClassTag,
                      G <: GroupingCriteria: ToGroupByQuery: ClassTag,
                      T <: AggDecl: ToAggregateQuery: ClassTag,
                      R <: AggRes: ToHavingFilter: ClassTag,
                      Comb](implicit spark: SparkSession, ev: T =:!= DNil): ToGroupByAgg[A, G, T, R, Comb] =
    new ToGroupByAgg[A, G, T, R, Comb] {
      implicit val groupingCriteriaEncoder: Encoder[G] = Encoders.kryo[G]
      implicit val aggDeclEncoder: Encoder[T]          = Encoders.kryo[T]
      implicit val aggResEncoder: Encoder[R]           = Encoders.kryo[R]
      implicit val tupleEncoder: Encoder[(A, G, T)]    = Encoders.kryo[(A, G, T)]
      import spark.implicits._
      override def apply(
          query: Query[A, G, T, R, Comb]
      ): Dataset[A] => DataFrame = { da =>
        val groupByQuery    = ToGroupByQuery[G].apply()
        val aggregateQuery  = ToAggregateQuery[T].apply()
        val havingQuery     = ToHavingFilter[R].apply()
        val having          = query.havingF
        val orderCriterias  = query.orderCriterias
        val orderAggregates = query.orderResults
        val orderRecords    = query.orderRecords

        val orderings: List[Column] =
          orderRecords.map(_ => col("_1")).toList ++
            orderCriterias.map(_ => col("_2")) ++
            orderAggregates.map(_ => col("_3"))

        da.filter(a => query.filterF(a))
          .map { a =>
            val G = query.getG(a)
            val T = query.getT(a)
            (a, G, T)
          }
          .groupBy(groupByQuery: _*)
          .agg(aggregateQuery.head, aggregateQuery.tail: _*)
          .filterMany(havingQuery.head, havingQuery.tail: _*)
          .orderBy(orderings: _*)
      }
    }
}
