package com.github.trembita.ql

import cats._
import cats.implicits._
import shapeless._
import scala.annotation.implicitNotFound

/**
  * Represents some value A tagged with type U
  * It has a runtime representation unlike shapeless.@@
  *
  * @tparam A - type of the value
  * @tparam U - tag
  * @param value - the value itself
  **/
case class :@[A, U](value: A)

object :@ {

  /** {{{Eq[A :@ U] == Eq[A] }}} */
  implicit def taggedEq[A: Eq, U]: Eq[A :@ U] = new Eq[A :@ U] {
    def eqv(x: :@[A, U], y: :@[A, U]): Boolean = x.value === y.value
  }
}

/**
  * A wrapper for [[:@]]
  * used in aggregations
  *
  * @tparam A    - type of the value
  * @tparam U    - tag
  * @tparam AggT - type of the aggregation function
  * @param tagged - tagged value
  **/
case class TaggedAgg[A, U, AggT <: AggFunc.Type](tagged: A :@ U)

/**
  * Sum type
  * representing a list of some grouping criterias
  * used in [[QueryResult]]
  **/
sealed trait GroupingCriteria extends Product with Serializable {
  type Head
  type Tail <: GroupingCriteria
  def head: Head
  def tail: Tail
}

object GroupingCriteria {

  /** An empty GroupingCriteria (like [[shapeless.HNil]]) */
  sealed trait GNil extends GroupingCriteria {
    type Head = GNil
    type Tail = GNil
    def head: GNil = GNil
    def tail: GNil = GNil
    def &::[GH <: :@[_, _]](head: GH): GH &:: GNil =
      GroupingCriteria.&::(head, this)
  }
  final case object GNil extends GNil

  /** An cons type (like [[shapeless.::]]) */
  final case class &::[GH <: :@[_, _], GT <: GroupingCriteria](head: GH, tail: GT) extends GroupingCriteria {
    type Head = GH
    type Tail = GT
    override def toString: String = s"$head &:: $tail"
  }

  /** Copy-pasted from shapeless =) */
  @implicitNotFound(
    "Implicit not found: GroupingCriteria At[${L}, ${N}]. You requested to access an element at the position ${N}, but the GroupingCriteria ${L} is too short."
  )
  trait At[L <: GroupingCriteria, N <: Nat] extends DepFn1[L] with Serializable

  object At {
    def apply[L <: GroupingCriteria, N <: Nat](
        implicit at: At[L, N]
    ): Aux[L, N, at.Out] = at

    type Aux[L <: GroupingCriteria, N <: Nat, Out0] = At[L, N] {
      type Out = Out0
    }

    implicit def groupingCriteriaAtZero[H <: :@[_, _], T <: GroupingCriteria]: Aux[H &:: T, _0, H] =
      new At[H &:: T, _0] {
        type Out = H
        def apply(l: H &:: T): Out = l.head
      }

    implicit def groupingCriteriaAtN[H <: :@[_, _], T <: GroupingCriteria, N <: Nat, AtOut](
        implicit att: At.Aux[T, N, AtOut]
    ): Aux[H &:: T, Succ[N], AtOut] =
      new At[H &:: T, Succ[N]] {
        type Out = AtOut
        def apply(l: H &:: T): Out = att(l.tail)
      }
  }

  /** GNil as always equal to GNil...  */
  implicit object GNilEq extends Eq[GNil] {
    override def eqv(x: GNil, y: GNil): Boolean = true
  }
}

/**
  * Represents an aggregation function
  * For some type [[A]]
  * using combiner [[Comb]]
  * and producing an ouput [[Out]]
  * (this is also a [[Monoid]] for [[Comb]])
  *
  * @tparam A    - input type
  * @tparam Out  - output type
  * @tparam Comb - combiner type
  **/
@implicitNotFound(
  """Implicit value not found:
AggFunc[${A}, ${Out}, ${Comb}]...
Add {{{ com.github.trembita.ql._ }}} to your imports
and check required implicits in your scope:
- cats.Monoid[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.Sum]
- algebra.ring.Field[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.Avg]
- algebra.ring.Rng[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.Product]
- scala.Ordering[Ax] & com.github.trembita.ql.Default[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.Min | AggFunc.Type.Max]
- algebra.ring.Field[Ax] & spire.algebra.NRoot[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.STDEV | AggFunc.Type.RMS]
"""
)
trait AggFunc[-A, +Out, Comb] {
  def empty: Comb

  /**
    * Produces a new combiner
    * from an existing one and some value [[A]]
    *
    * @param comb  - combiner
    * @param value - some [[A]]
    * @return - a new combiner
    **/
  def add(comb: Comb, value: A): Comb

  def combine(comb1: Comb, comb2: Comb): Comb

  /**
    * Extract an [[AggFunc.Result]]
    * from the combiner
    *
    * @param comb - combiner
    * @return - [[Out]] wrapped into [[AggFunc.Result]]
    **/
  def extract[AA <: A, O >: Out](comb: Comb): AggFunc.Result[AA, O, Comb]
}

object AggFunc {
  def apply[A, Out, Comb](
      implicit F: AggFunc[A, Out, Comb]
  ): AggFunc[A, Out, Comb] = F

  /**
    * Represents a result of [[AggFunc]] application
    *
    * @tparam In   - input type
    * @tparam Out  - output type
    * @tparam Comb - combiner type
    * @param result   - the output itself
    * @param combiner - a combiner (used for parallel computations)
    **/
  final case class Result[-In, Out, Comb](result: Out, combiner: Comb)

  /**
    * An extendable sum type
    * representing some type of aggregations
    **/
  trait Type
  object Type {
    sealed trait Sum     extends Type
    sealed trait Count   extends Type
    sealed trait Avg     extends Type
    sealed trait Max     extends Type
    sealed trait Min     extends Type
    sealed trait Product extends Type
    sealed trait Random  extends Type

    /** Collect values into collection */
    sealed trait Arr extends Type

    /** Concatenate string representations of the values */
    sealed trait StringAgg extends Type

    /** Standard deviation */
    sealed trait STDEV extends Type

    /** Root mean square */
    sealed trait RMS extends Type
  }
  trait types {
    val sum: Type.Sum             = new Type.Sum       {}
    val count: Type.Count         = new Type.Count     {}
    val avg: Type.Avg             = new Type.Avg       {}
    val max: Type.Max             = new Type.Max       {}
    val min: Type.Min             = new Type.Min       {}
    val product: Type.Product     = new Type.Product   {}
    val random: Type.Random       = new Type.Random    {}
    val arr: Type.Arr             = new Type.Arr       {}
    val stringAgg: Type.StringAgg = new Type.StringAgg {}
    val stdev: Type.STDEV         = new Type.STDEV     {}
    val rms: Type.RMS             = new Type.RMS       {}
  }
}

/**
  * [[shapeless.HList]] like type
  * representing a declaration for aggregations to be done
  **/
sealed trait AggDecl extends Product with Serializable
object AggDecl {

  /** Empty declaration (like [[shapeless.HNil]]) */
  sealed trait DNil extends AggDecl {
    def %::[DH <: TaggedAgg[_, _, _]](head: DH): DH %:: DNil =
      AggDecl.%::(head, this)
  }
  final case object DNil extends DNil

  /**
    * Cons type for aggregation declarations
    * (like [[shapeless.::]])
    *
    * @tparam DH - type of the first declaration (must be [[TaggedAgg]])
    * @tparam DT - type of the rest of declarations
    * @param head - first declaration
    * @param tail - rest of declarations
    **/
  final case class %::[DH <: TaggedAgg[_, _, _], DT <: AggDecl](head: DH, tail: DT) extends AggDecl {
    override def toString: String = s"$head %:: $tail"
  }
}

/**
  * [[shapeless.HList]] like type
  * representing a result of some aggregations
  **/
sealed trait AggRes extends Product with Serializable
object AggRes {

  /** Empty aggregation result (like [[shapeless.HNil]]) */
  sealed trait RNil extends AggRes {
    def *::[RH <: :@[_, _]](head: RH): RH *:: RNil = AggRes.*::(head, this)
  }
  final case object RNil extends RNil

  /**
    * Cons type for aggregation declarations
    * (like [[shapeless.::]])
    *
    * @tparam RH - type of the first result (must be [[:@]])
    * @tparam RT - type of the rest of results
    * @param head - first result
    * @param tail - rest of results
    **/
  final case class *::[RH <: :@[_, _], RT <: AggRes](head: RH, tail: RT) extends AggRes {
    override def toString: String = s"$head *:: $tail"
  }

  /**
    * Allows to get a some value tagged as [[U]]
    * from some [[L]] <: [[AggRes]]
    **/
  @implicitNotFound("${L} does'nt contains a value tagged with ${U}")
  trait Get[L <: AggRes, U] extends DepFn1[L] with Serializable

  object Get {
    def apply[L <: AggRes, Out](implicit at: Get[L, Out]): Aux[L, Out, at.Out] =
      at

    type Aux[L <: AggRes, U, Out0] = Get[L, U] { type Out = Out0 }

    implicit def fromHead[A, U, T <: AggRes]: Aux[(A :@ U) *:: T, U, A] =
      new Get[(A :@ U) *:: T, U] {
        type Out = A
        def apply(t: *::[A :@ U, T]): Out = t.head.value
      }

    implicit def fromTail[H <: :@[_, _], T <: AggRes, U, AtOut](
        implicit att: Get.Aux[T, U, AtOut]
    ): Aux[H *:: T, U, AtOut] =
      new Get[H *:: T, U] {
        type Out = AtOut
        def apply(t: *::[H, T]): Out = att(t.tail)
      }
  }
}

/**
  * Builder for a query used in [[trembitaql]]
  *
  * @tparam A - record type
  **/
sealed trait QueryBuilder[A]
object QueryBuilder {

  /**
    * An empty builder
    *
    * @tparam A - record type
    **/
  class Empty[A]() extends QueryBuilder[A] {

    /**
      * Like Where clause in SQL
      *
      * @param p - predicate
      **/
    def where(p: A => Boolean): Where[A] = new Where(p)

    /**
      * Like Group By clause in SQL
      *
      * @tparam G - a grouping criteria
      * @param getG - extract [[G]] from record [[A]]
      **/
    def groupBy[T, G <: GroupingCriteria](magnet: ExprMagnet.Aux[T, A => G]): GroupBy[A, G] =
      new GroupBy[A, G](magnet(), None)
  }

  class Where[A](val p: A => Boolean) extends QueryBuilder[A] {
    def filter(p2: A => Boolean): Where[A] =
      new Where((a: A) => p(a) && p2(a))

    def groupBy[T, G <: GroupingCriteria](magnet: ExprMagnet.Aux[T, A => G]): GroupBy[A, G] =
      new GroupBy[A, G](magnet(), Some(this))
  }

  class GroupBy[A, G <: GroupingCriteria](val getG: A => G, val filterOpt: Option[Where[A]]) extends QueryBuilder[A] {

    /**
      * An arbitrary aggregation
      *
      * @tparam T    - aggregation declaration
      * @tparam R    - expected aggregation result
      * @tparam Comb - combiner used in aggregations
      * @param getT - get an aggregation declaration from record
      * @param aggF - [[AggFunc]] for types [[T]], [[R]], [[Comb]]
      **/
    def aggregate[T, D <: AggDecl, R <: AggRes, Comb](magnet: ExprMagnet.Aux[T, A => D])(
        implicit aggF: AggFunc[D, R, Comb]
    ): Aggregate[A, G, D, R, Comb] =
      new Aggregate(getG, magnet(), filterOpt)
  }

  class Aggregate[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
      val getG: A => G,
      val getT: A => T,
      val filterOpt: Option[Where[A]]
  )(implicit val aggF: AggFunc[T, R, Comb])
      extends QueryBuilder[A] {

    /**
      * Like Having clause in SQL
      *
      * @param p - predicate
      **/
    def having[U](magnet: ExprMagnet.Aux[U, R => Boolean]): MaybeOrderedHaving[A, G, T, R, Comb] =
      new MaybeOrderedHaving(getG, getT, magnet(), filterOpt, None, None, None)

    /**
      * Like Order By clause in SQL
      * for record [[A]]
      *
      * @param Ord - implicit [[Ordering]] for [[A]]
      **/
    def orderRecords(
        implicit Ord: Ordering[A]
    ): MaybeOrderedHaving[A, G, T, R, Comb] =
      new MaybeOrderedHaving(
        getG,
        getT,
        (_: R) => true,
        filterOpt,
        Some(Ord),
        None,
        None
      )

    /**
      * Like Order By clause in SQL
      * ordering records of type [[A]]
      * by some criteria [[B]]
      * having an implicit [[Ordering]] for [[B]]
      *
      * @param f - extract an ordering criteria
      **/
    def orderRecordsBy[B: Ordering](
        f: A => B
    ): MaybeOrderedHaving[A, G, T, R, Comb] = orderRecords(Ordering.by[A, B](f))

    /**
      * Like Order By clause in SQL
      * for grouping criteria [[G]]
      *
      * @param Ord - implicit [[Ordering]] for [[G]]
      **/
    def orderGroups(
        implicit Ord: Ordering[G]
    ): MaybeOrderedHaving[A, G, T, R, Comb] =
      new MaybeOrderedHaving(
        getG,
        getT,
        (_: R) => true,
        filterOpt,
        None,
        Some(Ord),
        None
      )

    /**
      * Like Order By clause in SQL
      * ordering grouping criteria of type [[G]]
      * by some criteria [[B]]
      * having an implicit [[Ordering]] for [[B]]
      *
      * @param f - extract an ordering criteria
      **/
    def orderGroupsBy[B: Ordering](
        f: G => B
    ): MaybeOrderedHaving[A, G, T, R, Comb] = orderGroups(Ordering.by[G, B](f))

    /**
      * Like Order By clause in SQL
      * for aggregation result [[R]]
      *
      * @param Ord - implicit [[Ordering]] for [[R]]
      **/
    def orderAggregations(
        implicit Ord: Ordering[R]
    ): MaybeOrderedHaving[A, G, T, R, Comb] =
      new MaybeOrderedHaving(
        getG,
        getT,
        (_: R) => true,
        filterOpt,
        None,
        None,
        Some(Ord)
      )

    /**
      * Like Order By clause in SQL
      * ordering aggregation result [[R]]
      * by some criteria [[B]]
      * having an implicit [[Ordering]] for [[B]]
      *
      * @param f - extract an ordering criteria
      **/
    def orderAggregationsBy[B: Ordering](
        f: R => B
    ): MaybeOrderedHaving[A, G, T, R, Comb] =
      orderAggregations(Ordering.by[R, B](f))

    /**
      * Order all QueryResuts
      * having [[Ordering]] for [[A]], [[G]] and [[R]]
      *
      * @param OrdA - implicit [[Ordering]] for [[A]]
      * @param OrdG - implicit [[Ordering]] for [[G]]
      * @param OrdR - implicit [[Ordering]] for [[R]]
      **/
    def ordered(implicit OrdA: Ordering[A], OrdG: Ordering[G], OrdR: Ordering[R]): MaybeOrderedHaving[A, G, T, R, Comb] =
      new MaybeOrderedHaving(
        getG,
        getT,
        (_: R) => true,
        filterOpt,
        Some(OrdA),
        Some(OrdG),
        Some(OrdR)
      )
  }

  class MaybeOrderedHaving[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
      override val getG: A => G,
      override val getT: A => T,
      val havingF: R => Boolean,
      override val filterOpt: Option[Where[A]],
      val orderRecords: Option[Ordering[A]],
      val orderGroups: Option[Ordering[G]],
      val orderResults: Option[Ordering[R]]
  )(override implicit val aggF: AggFunc[T, R, Comb])
      extends Aggregate[A, G, T, R, Comb](getG, getT, filterOpt)

  /** * A query ready to use by [[trembitaql]]* */
  final case class Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
      getG: A => G,
      getT: A => T,
      filterF: A => Boolean,
      havingF: R => Boolean,
      orderRecords: Option[Ordering[A]],
      orderCriterias: Option[Ordering[G]],
      orderResults: Option[Ordering[R]]
  )(implicit val aggF: AggFunc[T, R, Comb])

}
