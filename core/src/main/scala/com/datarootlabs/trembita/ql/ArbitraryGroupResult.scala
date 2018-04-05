package com.datarootlabs.trembita.ql


import scala.language.higherKinds
import cats.Monoid
import com.datarootlabs.trembita.ql.GroupingCriteria._


sealed trait ArbitraryGroupResult[-A, K <: GroupingCriteria, +T] {
  type SubGroup <: ArbitraryGroupResult[A, K#Tail, T]
  def subGroup: SubGroup
  def key: K#Key
  def totals: T
  def sameKey[
  AA <: A,
  TT >: T
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
  T
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
  T
  ](totals: T, records: ArbitraryGroupResult[A, K, T]*) extends ArbitraryGroupResult[A, K, T] {
    type SubGroup = ArbitraryGroupResult[A, K#Tail, T]
    def subGroup: SubGroup = ???
    def key: K#Key = ???
  }
}
