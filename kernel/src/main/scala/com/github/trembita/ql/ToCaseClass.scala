package com.github.trembita.ql

import shapeless._
import shapeless.ops.hlist.Prepend
import scala.language.experimental.macros

trait ToCaseClass[A, K <: GroupingCriteria, T]
    extends DepFn1[QueryResult[A, K, T]]

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
                      L <: HList,
                      R](implicit ev: Generic.Aux[R, Vector[A] :: L],
                         kToHList: ToHList.Aux[K, KR],
                         tToHList: ToHList.Aux[T, TR],
                         prepend0: Prepend.Aux[KR, TR, L],
  ): ToCaseClass.Aux[A, K, T, R] = new ToCaseClass[A, K, T] {
    type Out = R
    def apply(t: QueryResult[A, K, T]): R = {
      val totalsHList = tToHList(t.totals)
      val keysHList = kToHList(t.keys)
      val prepended = prepend0(keysHList, totalsHList)
      ev.from(t.values :: prepended)
    }
  }
}
