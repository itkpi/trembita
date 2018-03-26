package com.datarootlabs.trembita.ql


import cats.Monoid
import cats.implicits._
import shapeless._


// @formatter:off
case class GroupWithTotalResult[K, T <: HList
                                      : Monoid,
                                   A](key: K, totals: T, records: Iterable[A]) {
  def ++ (that: GroupWithTotalResult[K, T, A]): GroupWithTotalResult[K, T, A] = {
//    require(this.key == that.key, s"Could not merge 'GroupWithTotalResult's for different keys: [$key, ${that.key}]")
    GroupWithTotalResult(key, this.totals |+| that.totals, this.records ++ that.records)
  }
}

case class GroupWithTotal2Result[K1, K2, T <: HList
                                            : Monoid,
                                         A](key: K1, totals: T, records: Iterable[GroupWithTotalResult[K2, T, A]]) {
  def ++ (that: GroupWithTotal2Result[K1, K2, T, A]): GroupWithTotal2Result[K1, K2, T, A] = {
//    require(this.key == that.key, s"Could not merge 'GroupWithTotal2Result's for different key pairs: [$key, ${that.key}]")
    GroupWithTotal2Result(key, this.totals |+| that.totals, this.records ++ that.records)
  }

  def :+(groupByWithTotalResult: GroupWithTotalResult[K2, T, A]): GroupWithTotal2Result[K1, K2, T, A] = {
    GroupWithTotal2Result(key, this.totals |+| groupByWithTotalResult.totals,
      records ++: Some(groupByWithTotalResult))
  }
}