package com.github.trembita.ql

import shapeless._
import shapeless.ops.hlist.Prepend
import scala.language.experimental.macros

trait ToCaseClass[A, K <: GroupingCriteria, T]
    extends DepFn1[QueryResult[A, K, T]] with Serializable

object ToCaseClass {
  type Aux[A, K <: GroupingCriteria, T, Out0] = ToCaseClass[A, K, T] {
    type Out = Out0
  }

  def apply[A, K <: GroupingCriteria, T](
    implicit ev: ToCaseClass[A, K, T]
  ): Aux[A, K, T, ev.Out] = ev

  implicit def derive[A,
                      K <: GroupingCriteria,
                      KR <: HList,
                      T,
                      TR <: HList,
                      P <: HList,
                      L <: HList,
                      R](implicit kToHList: ToHList.Aux[K, KR],
                         tToHList: ToHList.Aux[T, TR],
                         prepend0: Prepend.Aux[KR, TR, L],
                         prepend1: Prepend.Aux[L, Vector[A] :: HNil, P],
                         ev: Generic.Aux[R, P]): ToCaseClass.Aux[A, K, T, R] =
    new ToCaseClass[A, K, T] {
      type Out = R
      def apply(t: QueryResult[A, K, T]): R = {
        val totalsHList = tToHList(t.totals)
        val keysHList = kToHList(t.keys)
        val prepended0 = prepend0(keysHList, totalsHList)
        val repr = prepend1(prepended0, t.values :: HNil)
        ev.from(repr)
      }
    }
}
