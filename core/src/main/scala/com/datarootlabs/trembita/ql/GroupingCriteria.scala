package com.datarootlabs.trembita.ql

import scala.language.{existentials, higherKinds, implicitConversions}
import cats._
import cats.implicits._
import algebra.ring._
import shapeless._

import scala.annotation.implicitNotFound
import scala.reflect.macros.blackbox


case class ##[A, U](value: A) {
  type Value = A
  type Tag = U
}
case class TaggedAgg[A, U, AggT <: AggFunc.Type](tagged: A ## U) {
  type Value = A
  type Tag = U
}

sealed trait GroupingCriteria extends Product with Serializable {
  type Key
  type Tail <: GroupingCriteria
  def key: Key
  def tail: Tail
}

object GroupingCriteria {
  implicit object GNilEq extends Eq[GNil] {
    override def eqv(x: GNil, y: GNil): Boolean = true
  }

  sealed trait GNil extends GroupingCriteria {
    type Key = GNil
    type Tail = GNil
    def key: GNil = GNil
    def tail: GNil = GNil
    def &::[GH <: ##[_, _]](head: GH): GH &:: GNil = GroupingCriteria.&::(head, this)
  }
  case object GNil extends GNil

  case class &::[GH <: ##[_, _], GT <: GroupingCriteria](first: GH, rest: GT) extends GroupingCriteria {
    type Key = GH
    type Tail = GT
    val key : GH = first
    val tail: GT = rest
    override def toString: String = s"$first &:: $rest"
  }

  @implicitNotFound("Implicit not found: GroupingCriteria At[${L}, ${N}]. You requested to access an element at the position ${N}, but the GroupingCriteria ${L} is too short.")
  trait At[L <: GroupingCriteria, N <: Nat] extends DepFn1[L] with Serializable

  object At {
    def apply[L <: GroupingCriteria, N <: Nat](implicit at: At[L, N]): Aux[L, N, at.Out] = at

    type Aux[L <: GroupingCriteria, N <: Nat, Out0] = At[L, N] {type Out = Out0}

    implicit def hlistAtZero[H <: ##[_, _], T <: GroupingCriteria]: Aux[H &:: T, _0, H] =
      new At[H &:: T, _0] {
        type Out = H
        def apply(l: H &:: T): Out = l.first
      }

    implicit def hlistAtN[H <: ##[_, _], T <: GroupingCriteria, N <: Nat, AtOut]
    (implicit att: At.Aux[T, N, AtOut]): Aux[H &:: T, Succ[N], AtOut] =
      new At[H &:: T, Succ[N]] {
        type Out = AtOut
        def apply(l: H &:: T): Out = att(l.rest)
      }
  }
}


protected[trembita] trait AggFunc[-A, +Out] {
  type Comb
  def empty: Comb
  def add(comb: Comb, value: A): Comb
  def combine(comb1: Comb, comb2: Comb): Comb
  def extract[AA <: A, O >: Out](comb: Comb): AggFunc.Result[AA, O, Comb]

  def foldLeft[AA <: A, O >: Out](vs: Seq[A]): AggFunc.Result[AA, O, Comb] = extract(vs.foldLeft(empty)(add))
}
object AggFunc {
  case class Result[-In, Out, +Comb](result: Out, combiner: Comb)
  def apply[A, Out](implicit F: AggFunc[A, Out]): AggFunc[A, Out] = F

  sealed trait Type
  object Type {
    sealed trait Sum extends Type
    sealed trait Count extends Type
    sealed trait Avg extends Type
  }
}

sealed trait AggDecl extends Product with Serializable
object AggDecl {
  sealed trait DNil extends AggDecl {
    def %::[DH <: TaggedAgg[_, _, _]](head: DH): DH %:: DNil = AggDecl.%::(head, this)
  }
  case object DNil extends DNil
  case class %::[DH <: TaggedAgg[_, _, _], DT <: AggDecl](head: DH, tail: DT) extends AggDecl {
    override def toString: String = s"$head %:: $tail"
  }
}

sealed trait AggRes extends Product with Serializable
object AggRes {
  sealed trait RNil extends AggRes {
    def *::[RH <: ##[_, _]](head: RH): RH *:: RNil = AggRes.*::(head, this)
  }
  case object RNil extends RNil
  case class *::[RH <: ##[_, _], RT <: AggRes](head: RH, tail: RT) extends AggRes {
    override def toString: String = s"$head *:: $tail"
  }

  @implicitNotFound("${L} does'nt contains a value tagged with ${U}")
  trait Get[L <: AggRes, U] extends DepFn1[L] with Serializable

  object Get {
    def apply[L <: AggRes, Out](implicit at: Get[L, Out]): Aux[L, Out, at.Out] = at

    type Aux[L <: AggRes, U, Out0] = Get[L, U] {type Out = Out0}

    implicit def fromHead[A, U, T <: AggRes]: Aux[(A ## U) *:: T, U, A] =
      new Get[(A ## U) *:: T, U] {
        type Out = A
        def apply(t: *::[(A ## U), T]): Out = t.head.value
      }

    implicit def fromTail[H <: ##[_, _], T <: AggRes, U, AtOut]
    (implicit att: Get.Aux[T, U, AtOut]): Aux[H *:: T, U, AtOut] =
      new Get[H *:: T, U] {
        type Out = AtOut
        def apply(t: *::[H, T]): Out = att(t.tail)
      }
  }
}

sealed trait QueryDone_?
sealed trait Yes extends QueryDone_?
sealed trait NO extends QueryDone_?
sealed trait QueryBuilder[A, D <: QueryDone_?]
object QueryBuilder {

  class Empty[A] protected[trembita]() extends QueryBuilder[A, NO] {
    def filter(p: A ⇒ Boolean): Filter[A] = new Filter(p)
    def groupBy[G <: GroupingCriteria](getG: A ⇒ G): GroupBy[A, G] = new GroupBy(getG, None)
  }

  class Filter[A] protected[trembita](protected[trembita] val p: A ⇒ Boolean) extends QueryBuilder[A, NO] {
    def filter(p2: A ⇒ Boolean): Filter[A] = new Filter((a: A) ⇒ p(a) && p2(a))
    def groupBy[G <: GroupingCriteria](getG: A ⇒ G): GroupBy[A, G] = new GroupBy(getG, Some(this))
  }

  class GroupBy[A, G <: GroupingCriteria] protected[trembita]
  (protected[trembita] val getG: A ⇒ G,
   protected[trembita] val filterOpt: Option[Filter[A]]) extends QueryBuilder[A, NO] {
    def aggregate[T <: AggDecl, R <: AggRes](getT: A ⇒ T)(implicit aggF: AggFunc[T, R]): Aggregate[A, G, T, R] =
      new Aggregate(getG, getT, filterOpt)
  }

  class Aggregate[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes] protected[trembita]
  (protected[trembita] val getG: A ⇒ G,
   protected[trembita] val getT: A ⇒ T,
   protected[trembita] val filterOpt: Option[Filter[A]])
  (protected[trembita] implicit val aggF: AggFunc[T, R]) extends QueryBuilder[A, Yes] {
    def having(p: R ⇒ Boolean): Having[A, G, T, R] = new Having(getG, getT, p, filterOpt)
  }

  class Having[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes] protected[trembita]
  (protected[trembita] val getG: A ⇒ G,
   protected[trembita] val getT: A ⇒ T,
   protected[trembita] val havingF: R ⇒ Boolean,
   protected[trembita] val filterOpt: Option[Filter[A]])
  (protected[trembita] implicit val aggF: AggFunc[T, R]) extends QueryBuilder[A, Yes] {
    def having(p: R ⇒ Boolean): Having[A, G, T, R] = new Having(getG, getT, (r: R) ⇒ havingF(r) && p(r), filterOpt)
  }

  case class Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes]
  (getG: A ⇒ G, getT: A ⇒ T, filterF: A ⇒ Boolean, havingF: R ⇒ Boolean)(implicit val aggF: AggFunc[T, R]) {
    type Comb = aggF.Comb
  }

  object Query {
    implicit def agg2Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes](h: Aggregate[A, G, T, R]): Query[A, G, T, R] =
      Query(h.getG, h.getT, (a: A) ⇒ h.filterOpt.forall(_.p(a)), (_: R) ⇒ true)(h.aggF)

    implicit def having2Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes](h: Having[A, G, T, R]): Query[A, G, T, R] =
      Query(h.getG, h.getT, (a: A) ⇒ h.filterOpt.forall(_.p(a)), h.havingF)(h.aggF)
  }
}