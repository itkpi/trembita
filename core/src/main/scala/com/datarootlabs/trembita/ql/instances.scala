package com.datarootlabs.trembita.ql


import cats._
import shapeless.HList

import scala.language.experimental.macros


trait instances
  extends /*aggregationInstances
  with */ Orderings

object instances extends instances {
  //  implicit def aggMonoid[K <: Aggregation]: Monoid[K] = macro aggMonoidImpl[K]
  //  implicit def arbitraryGroupResultMonoid[A, K <: GroupingCriteria, T <: AggDecl]: Monoid[ArbitraryGroupResult[A, K, T]] = macro arbitraryGroupResultMonoidImpl[A, K, T]
}
