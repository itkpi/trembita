package com.datarootlabs.trembita.ql


import cats._
import shapeless.HList

import scala.language.experimental.macros


trait instances
  extends MonoidInstances
  with EmptyInstances
  with Orderings

object instances extends instances {
  implicit def hListEmpty[K <: HList]: Empty[K] = macro hListEmptyImpl[K]
  implicit def hListMonoid[K <: HList]: Monoid[K] = macro hListMonoidImpl[K]
  implicit def aggMonoid[K <: Aggregation]: Monoid[K] = macro aggMonoidImpl[K]
  implicit def arbitraryGroupResultMonoid[A, K <: GroupingCriteria, T <: Aggregation]: Monoid[ArbitraryGroupResult[A, K, T]] = macro arbitraryGroupResultMonoidImpl[A, K, T]
}
