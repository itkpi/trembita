package com.datarootlabs.trembita.ql


import cats.Monoid
import com.datarootlabs.trembita.ql.Aggregation.AgNil
import com.datarootlabs.trembita.ql.GroupingCriteria._
import shapeless._
import shapeless.tag.@@

import scala.language.higherKinds


sealed trait ArbitraryGroupResult[-A, K <: GroupingCriteria, +T <: Aggregation] {
  type SubGroup <: ArbitraryGroupResult[A, K#Tail, T]
  def subGroup: SubGroup
  def key: K#Key
  def totals: T
  def sameKey[
  AA <: A,
  TT >: T <: Aggregation
  ](that: ArbitraryGroupResult[AA, K, TT]): Boolean = this.key == that.key
}

object ArbitraryGroupResult {
  case class ##@[A](records: A*) extends ArbitraryGroupResult[A, GNil, Nothing] {
    type SubGroup = this.type
    def subGroup: SubGroup = this
    def key: GNil#Key = GNil
    def totals: Nothing = ???
  }

  case class ~::[
  A,
  KH <: ##[_, _],
  KT <: GroupingCriteria,
  T <: Aggregation
  ](
     key: KH,
     totals: T,
     subGroup: ArbitraryGroupResult[A, KT, T]
   ) extends ArbitraryGroupResult[A, KH &:: KT, T] {
    type SubGroup = ArbitraryGroupResult[A, KT, T]
  }

  case class ~**[
  A,
  K <: GroupingCriteria,
  T <: Aggregation
  ](records: ArbitraryGroupResult[A, K, T]*)
   (implicit tMonoid: Monoid[T]) extends ArbitraryGroupResult[A, K, T] {
    type SubGroup = ArbitraryGroupResult[A, K#Tail, T]
    def subGroup: SubGroup = ~**(records.map(_.subGroup): _*)
    def key: K#Key = MultiKey[K#Key]()
    def totals: T = records.foldLeft(tMonoid.empty) { case (acc, r) ⇒ tMonoid.combine(acc, r.totals) }
  }
}