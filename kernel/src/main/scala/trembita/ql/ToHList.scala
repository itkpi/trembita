package trembita.ql

import trembita.ql.AggDecl.{%::, DNil}
import trembita.ql.AggRes.{*::, RNil}
import trembita.ql.GroupingCriteria.{&::, GNil}
import shapeless._
import scala.annotation.implicitNotFound
import scala.language.experimental.macros

@implicitNotFound("""Unable to convert ${A} to HList""")
trait ToHList[A] extends DepFn1[A] with Serializable { type Out <: HList }

object ToHList {
  type Aux[A, Out0 <: HList] = ToHList[A] { type Out = Out0 }
  def apply[A](implicit ev: ToHList[A]): Aux[A, ev.Out] = ev

  implicit val gnilToHList: ToHList.Aux[GNil, HNil] = new ToHList[GNil] {
    type Out = HNil
    def apply(t: GNil): HNil = HNil
  }

  implicit def gconsToHList[A, U, RT <: GroupingCriteria, L <: HList](
      implicit ev: ToHList.Aux[RT, L]
  ): ToHList.Aux[(A :@ U) &:: RT, A :: L] =
    new ToHList[(A :@ U) &:: RT] {
      type Out = A :: L
      def apply(t: (A :@ U) &:: RT): Out = t.head.value :: ev(t.tail)
    }

  implicit def aggFuncResToHList[In, R, Comb](
      implicit ev: ToHList[R]
  ): ToHList.Aux[AggFunc.Result[In, R, Comb], ev.Out] =
    new ToHList[AggFunc.Result[In, R, Comb]] {
      type Out = ev.Out
      def apply(t: AggFunc.Result[In, R, Comb]): Out = ev(t.result)
    }

  implicit val rnilToHList: ToHList.Aux[RNil, HNil] = new ToHList[RNil] {
    type Out = HNil
    def apply(t: RNil): HNil = HNil
  }

  implicit def rconsToHList[A, U, RT <: AggRes, L <: HList](
      implicit ev: ToHList.Aux[RT, L]
  ): ToHList.Aux[(A :@ U) *:: RT, A :: L] =
    new ToHList[(A :@ U) *:: RT] {
      type Out = A :: L
      def apply(t: (A :@ U) *:: RT): Out = t.head.value :: ev(t.tail)
    }
}

trait FromHList[A <: HList] extends DepFn1[A] with Serializable

object FromHList {
  @implicitNotFound("""Unable to create ${Out0} from ${A}""")
  type Aux[A <: HList, Out0] = FromHList[A] { type Out = Out0 }
  def apply[A <: HList](implicit ev: FromHList[A]): Aux[A, ev.Out] = ev

  implicit val hnilToGNil: FromHList.Aux[HNil, GNil] = new FromHList[HNil] {
    type Out = GNil
    def apply(t: HNil): GNil = GNil
  }

  implicit def hconsToGcons[A, U, L <: HList, RT <: GroupingCriteria](
      implicit ev: FromHList.Aux[L, RT]
  ): FromHList.Aux[(A :@ U) :: L, (A :@ U) &:: RT] =
    new FromHList[(A :@ U) :: L] {
      type Out = (A :@ U) &:: RT
      def apply(t: (A :@ U) :: L): Out = t.head &:: ev(t.tail)
    }

  implicit val hnilToDNil: FromHList.Aux[HNil, DNil] = new FromHList[HNil] {
    type Out = DNil
    def apply(t: HNil): DNil = DNil
  }

  implicit def hconsToDcons[A, U, T <: AggFunc.Type, L <: HList, DT <: AggDecl](
      implicit ev: FromHList.Aux[L, DT]
  ): FromHList.Aux[TaggedAgg[A, U, T] :: L, TaggedAgg[A, U, T] %:: DT] =
    new FromHList[TaggedAgg[A, U, T] :: L] {
      type Out = TaggedAgg[A, U, T] %:: DT
      def apply(t: TaggedAgg[A, U, T] :: L): Out = t.head %:: ev(t.tail)
    }
}
