package com.datarootlabs.trembita


import language.{higherKinds, implicitConversions}
import language.experimental.macros
import shapeless._
import ql.AggRes._
import ql.AggDecl._
import ql.QueryBuilder._
import ql.GroupingCriteria._


package object ql
  extends orderingInstances
  with aggregationInstances
  with monoidInstances {

  implicit class TaggingSyntax[A](val self: A) extends AnyVal {
    def as[T]: A :@ T = new :@[A, T](self)
    def :@[T]: A :@ T = new :@[A, T](self)
  }

  implicit class GroupingCriteriaOps[G <: GroupingCriteria](val self: G) extends AnyVal {
    def &::[GH <: :@[_, _]](head: GH): GH &:: G = GroupingCriteria.&::(head, self)
    def apply(n: Nat)(implicit at: GroupingCriteria.At[G, n.N]): at.Out = at(self)
  }

  implicit class AggregationNameOps[A <: AggDecl](val self: A) {
    def %::[GH <: TaggedAgg[_, _, _]](head: GH): GH %:: A = AggDecl.%::(head, self)
  }

  implicit class AggResOps[A <: AggRes](val self: A) {
    def *::[H <: :@[_, _]](head: H): H *:: A = AggRes.*::(head, self)
    def apply[U](implicit get: AggRes.Get[A, U]): get.Out = get(self)
    def get[U](implicit gget: AggRes.Get[A, U]): gget.Out = gget(self)
  }


  implicit class TaggingOps[A, U](val self: A :@ U) extends AnyVal {
    def sum: TaggedAgg[A, U, AggFunc.Type.Sum] = TaggedAgg(self)
    def avg: TaggedAgg[A, U, AggFunc.Type.Avg] = TaggedAgg(self)
    def count: TaggedAgg[A, U, AggFunc.Type.Count] = TaggedAgg(self)
    def max: TaggedAgg[A, U, AggFunc.Type.Max] = TaggedAgg(self)
    def min: TaggedAgg[A, U, AggFunc.Type.Min] = TaggedAgg(self)
    def product: TaggedAgg[A, U, AggFunc.Type.Product] = TaggedAgg(self)
    def arr: TaggedAgg[A, U, AggFunc.Type.Arr] = TaggedAgg(self)
    def stringAgg: TaggedAgg[A, U, AggFunc.Type.StringAgg] = TaggedAgg(self)
    def deviation: TaggedAgg[A, U, AggFunc.Type.STDEV] = TaggedAgg(self)
    def rms: TaggedAgg[A, U, AggFunc.Type.RMS] = TaggedAgg(self)
    def random: TaggedAgg[A, U, AggFunc.Type.Random] = TaggedAgg(self)
  }

  /** Trembita QL for [[Seq]] */
  implicit class TrembitaQL[A](val self: Seq[A]) extends AnyVal {
    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb]
    (queryF: Empty[A] ⇒ Query[A, G, T, R, Comb])
    (implicit trembitaql: trembitaql[A, G, T, R, Comb]): QueryResult[A, G, AggFunc.Result[T, R, Comb]] =
      trembitaql(self, queryF)
  }

//  /** Trembita QL for [[DataPipeline]] */
//  implicit class TrembitaQLForPipeline[A](val self: DataPipeline[A]) extends AnyVal {
//    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb]
//    (queryF: Empty[A] ⇒ Query[A, G, T, R, Comb])(implicit trembitaql: trembitaql[A, G, T, R, Comb])
//    : DataPipeline[QueryResult[A, G, AggFunc.Result[T, R, Comb]]] = DataPipeline.from({
//      val forced = self.eval
//      val result = trembitaql(forced.toSeq, queryF)
//      Seq(result)
//    })
//  }

  /**
    *
    * Implicit conversions bellow
    * guarantees correctness
    * of the query
    **/
  implicit def agg2Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb]
  (h: Aggregate[A, G, T, R, Comb]): Query[A, G, T, R, Comb] =
    Query[A, G, T, R, Comb](h.getG, h.getT, (a: A) ⇒ h.filterOpt.forall(_.p(a)), (_: R) ⇒ true, None, None, None)(h.aggF)

  implicit def ordered2Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb]
  (h: MaybeOrderedHaving[A, G, T, R, Comb]): Query[A, G, T, R, Comb] =
    Query[A, G, T, R, Comb](h.getG, h.getT, (a: A) ⇒ h.filterOpt.forall(_.p(a)), h.havingF,
      h.orderRecords, h.orderGroups, h.orderResults)(h.aggF)
}