package com.github.trembita

import cats._
import com.github.trembita.ql.AggFunc
import language.{higherKinds, implicitConversions}
import language.experimental.macros
import shapeless._
import ql.AggRes._
import ql.AggDecl._
import ql.QueryBuilder._
import ql.GroupingCriteria._
import shapeless.syntax.SingletonOps
import scala.reflect.ClassTag
import shapeless.{Widen, Witness}

package object ql
    extends orderingInstances
    with aggregationInstances
    with monoidInstances
    with spire.std.AnyInstances
    with AggFunc.types
    with exprMagnets
    with GroupingCriteriaFromTuple
    with AggDeclFromTuple {

  implicit class TaggingSyntax[A](private val self: A) extends AnyVal {
    def tagAs[T]: A :@ T = self.:@[T]
    def :@[T]: A :@ T    = new :@[A, T](self)
  }

  class aggDsl[A, U, AggT <: AggFunc.Type](val `f`: A => U) extends AnyVal {
    @inline def as(s: SingletonOps): A => TaggedAgg[U, s.T, AggT] = a => TaggedAgg(:@(`f`(a)))
  }
  class tagDsl[A, U](val `f`: A => U) extends AnyVal {
    @inline def as(s: SingletonOps): A => :@[U, s.T]                      = a => :@(`f`(a))
    @inline def agg[AggT <: AggFunc.Type](aggT: AggT): aggDsl[A, U, AggT] = new aggDsl[A, U, AggT](`f`)
  }
  class exprDsl[A](val `dummy`: Boolean = false) extends AnyVal {
    @inline def apply[U](f: A => U): tagDsl[A, U] = new tagDsl[A, U](f)
  }
  class havingDsl[A, T](val `f`: A => Boolean) extends AnyVal

  @inline def expr[A]: exprDsl[A]                                         = new exprDsl[A]()
  @inline def col[A]: tagDsl[A, A]                                        = new tagDsl[A, A](identity)
  @inline def agg[A](s: SingletonOps)(f: A => Boolean): havingDsl[A, s.T] = new havingDsl[A, s.T](f)

  implicit class GroupingCriteriaOps[G <: GroupingCriteria](private val self: G) extends AnyVal {
    def &::[GH <: :@[_, _]](head: GH): GH &:: G =
      GroupingCriteria.&::(head, self)

    def apply(n: Nat)(implicit at: GroupingCriteria.At[G, n.N]): at.Out =
      at(self)
  }

  implicit def tuple2GroupingCriteria[T, Out0 <: GroupingCriteria](t: T)(
      implicit ev: FromTuple.Aux[T, Out0]
  ): ev.Out = ev(t)

  implicit class AggregationNameOps[A <: AggDecl](val self: A) {
    def %::[GH <: TaggedAgg[_, _, _]](head: GH): GH %:: A =
      AggDecl.%::(head, self)
  }

  implicit def tuple2AggDecl[T, Out0 <: AggDecl](t: T)(
      implicit ev: FromTuple.Aux[T, Out0]
  ): ev.Out = ev(t)

  implicit class AggResOps[A <: AggRes](val self: A) {
    def *::[H <: :@[_, _]](head: H): H *:: A = AggRes.*::(head, self)

    def apply[U](u: U)(implicit get: AggRes.Get[A, U]): get.Out = get(self)

    def get[U](u: U)(implicit gget: AggRes.Get[A, U]): gget.Out = gget(self)
  }

  /** Trembita QL for [[DataPipelineT]] */
  implicit class TrembitaQLForPipeline[A, F[_], Ex <: Environment](
      private val self: DataPipelineT[F, A, Ex]
  ) extends AnyVal {
    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
        queryF: Empty[A] => Query[A, G, T, R, Comb]
    )(implicit trembitaql: trembitaql[A, G, T, R, Comb, Ex],
      ex: Ex,
      run: Ex#Run[F],
      F: Monad[F]): DataPipelineT[F, QueryResult[A, G, R], Ex] =
      trembitaql.apply(self, queryF)
  }

  implicit class AsOps[F[_], Ex <: Environment, A, G <: GroupingCriteria, T](
      private val self: DataPipelineT[F, QueryResult[A, G, T], Ex]
  ) extends AnyVal {
    def as[R: ClassTag](implicit ev: ToCaseClass.Aux[A, G, T, R], F: Monad[F]): DataPipelineT[F, R, Ex] =
      self.mapImpl(_.as[R])
  }

  /**
    *
    * Implicit conversions bellow
    * guarantees correctness
    * of the query
    **/
  implicit def agg2Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
      h: Aggregate[A, G, T, R, Comb]
  ): Query[A, G, T, R, Comb] =
    Query[A, G, T, R, Comb](
      h.getG,
      h.getT,
      (a: A) => h.filterOpt.forall(_.p(a)),
      (_: R) => true,
      None,
      None,
      None
    )(h.aggF)

  implicit def ordered2Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
      h: MaybeOrderedHaving[A, G, T, R, Comb]
  ): Query[A, G, T, R, Comb] =
    Query[A, G, T, R, Comb](
      h.getG,
      h.getT,
      (a: A) => h.filterOpt.forall(_.p(a)),
      h.havingF,
      h.orderRecords,
      h.orderGroups,
      h.orderResults
    )(h.aggF)

  implicit class QueryResultToCaseClass[A, K <: GroupingCriteria, T](
      private val self: QueryResult[A, K, T]
  ) extends AnyVal {
    def as[R](implicit ev: ToCaseClass.Aux[A, K, T, R]): R = ev(self)
  }
}
