package com.github.trembita.ql

import com.github.trembita.ql.AggRes.{*::, RNil}
import com.github.trembita.ql.GroupingCriteria.{&::, GNil}
import shapeless._
import scala.language.experimental.macros

trait ToHList[A] extends DepFn1[A] { type Out }

object ToHList {
  type Aux[A, Out0] = ToHList[A] { type Out = Out0 }
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
