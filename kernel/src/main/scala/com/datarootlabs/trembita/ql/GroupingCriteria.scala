package com.datarootlabs.trembita.ql

import cats._
import cats.implicits._
import shapeless._
import scala.annotation.implicitNotFound


case class ##[A, U](value: A) {
  type Value = A
  type Tag = U
}
object ## {
  implicit def taggedEq[A: Eq, U]: Eq[A ## U] = new Eq[A ## U] {
     def eqv(x: ##[A, U], y: ##[A, U]): Boolean = x.value === y.value
  }
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


@implicitNotFound("""Implicit value not found:
AggFunc[${A}, ${Out}, ${Comb}]...
Add {{{ com.datarootlabs.trembita.ql._ }}} to your imports
and check required implicits in your scope:
- cats.Monoid[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.Sum]
- algebra.ring.Field[Ax] ∀ type Ax used in TaggedAgg[Ax, _, AggFunc.Type.Avg]
""")
trait AggFunc[-A, +Out, Comb] {
  def empty: Comb
  def add(comb: Comb, value: A): Comb
  def combine(comb1: Comb, comb2: Comb): Comb
  def extract[AA <: A, O >: Out](comb: Comb): AggFunc.Result[AA, O, Comb]

  def foldLeft[AA <: A, O >: Out](vs: Seq[A]): AggFunc.Result[AA, O, Comb] = extract(vs.foldLeft(empty)(add))
}
object AggFunc {
  def apply[A, Out, Comb](implicit F: AggFunc[A, Out, Comb]): AggFunc[A, Out, Comb] = F

  case class Result[-In, Out, Comb](result: Out, combiner: Comb)

  trait Type
  object Type {
    sealed trait Sum extends Type
    sealed trait Count extends Type
    sealed trait Avg extends Type
    sealed trait Max extends Type
    sealed trait Min extends Type
    sealed trait Product extends Type
    sealed trait Arr extends Type
    sealed trait StringAgg extends Type
    sealed trait STDEV extends Type
    sealed trait STDEVP extends Type
    sealed trait VAR extends Type
    sealed trait VARP extends Type
    sealed trait RMS extends Type
    sealed trait ExpectedValue extends Type
    sealed trait Random extends Type
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

sealed trait QueryBuilder[A]
object QueryBuilder {

  class Empty[A] protected[trembita]() extends QueryBuilder[A] {
    def filter(p: A ⇒ Boolean): Filter[A] = new Filter(p)
    def groupBy[G <: GroupingCriteria](getG: A ⇒ G): GroupBy[A, G] = new GroupBy(getG, None)
  }

  class Filter[A] protected[trembita](protected[trembita] val p: A ⇒ Boolean) extends QueryBuilder[A] {
    def filter(p2: A ⇒ Boolean): Filter[A] = new Filter((a: A) ⇒ p(a) && p2(a))
    def groupBy[G <: GroupingCriteria](getG: A ⇒ G): GroupBy[A, G] = new GroupBy(getG, Some(this))
  }

  class GroupBy[A, G <: GroupingCriteria] protected[trembita]
  (protected[trembita] val getG: A ⇒ G,
   protected[trembita] val filterOpt: Option[Filter[A]]) extends QueryBuilder[A] {
    def aggregate[T <: AggDecl, R <: AggRes, Comb](getT: A ⇒ T)(implicit aggF: AggFunc[T, R, Comb]): Aggregate[A, G, T, R, Comb] =
      new Aggregate(getG, getT, filterOpt)
  }

  class Aggregate[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] protected[trembita]
  (protected[trembita] val getG: A ⇒ G,
   protected[trembita] val getT: A ⇒ T,
   protected[trembita] val filterOpt: Option[Filter[A]])
  (protected[trembita] implicit val aggF: AggFunc[T, R, Comb]) extends QueryBuilder[A] {
    def having(p: R ⇒ Boolean): Having[A, G, T, R, Comb] = new Having(getG, getT, p, filterOpt)
  }

  class Having[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] protected[trembita]
  (protected[trembita] val getG: A ⇒ G,
   protected[trembita] val getT: A ⇒ T,
   protected[trembita] val havingF: R ⇒ Boolean,
   protected[trembita] val filterOpt: Option[Filter[A]])
  (protected[trembita] implicit val aggF: AggFunc[T, R, Comb]) extends QueryBuilder[A] {
    def having(p: R ⇒ Boolean): Having[A, G, T, R, Comb] = new Having(getG, getT, (r: R) ⇒ havingF(r) && p(r), filterOpt)
  }

  case class Query[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb]
  (getG: A ⇒ G, getT: A ⇒ T, filterF: A ⇒ Boolean, havingF: R ⇒ Boolean)(implicit val aggF: AggFunc[T, R, Comb])

}