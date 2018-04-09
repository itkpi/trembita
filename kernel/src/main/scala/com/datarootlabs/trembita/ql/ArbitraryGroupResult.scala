package com.datarootlabs.trembita.ql


import com.datarootlabs.trembita.ql.GroupingCriteria._
import cats._
import cats.data.NonEmptyList
import cats.implicits._


sealed trait Key[+K] {

  import Key.{Multiple, Single}


  override def equals(obj: scala.Any): Boolean = (this, obj) match {
    case (Single(k1), Single(k2: K))                                  ⇒ k1 == k2
    case (Multiple(kh1, kt1), Multiple(kh2: K, kt2: NonEmptyList[K])) ⇒ kh1 == kh2 && kt1 == kt2
    case _                                                            ⇒ false
  }
}
object Key {
  sealed trait NoKey extends Key[Nothing]
  case object NoKey extends NoKey
  case class Single[K](value: K) extends Key[K]
  case class Multiple[K](head: K, tail: NonEmptyList[K]) extends Key[K]

  implicit def keyEq[K: Eq]: Eq[Key[K]] = new Eq[Key[K]] {
    override def eqv(x: Key[K], y: Key[K]): Boolean = (x, y) match {
      case (Single(k1), Single(k2))                 ⇒ k1 === k2
      case (Multiple(kh1, kt1), Multiple(kh2, kt2)) ⇒ kh1 === kh2 && kt1 === kt2
      case _                                        ⇒ false
    }
  }
  implicit def keyMonoid[K]: Monoid[Key[K]] = new Monoid[Key[K]] {
    def empty: Key[K] = NoKey
    def combine(x: Key[K], y: Key[K]): Key[K] = (x, y) match {
      case (NoKey, _)                             ⇒ y
      case (_, NoKey)                             ⇒ x
      case (Single(v1), Single(v2))               ⇒ Multiple(v1, NonEmptyList(v2, Nil))
      case (Single(v1), Multiple(v2, vs))         ⇒ Multiple(v1, v2 :: vs)
      case (Multiple(v2, vs), Single(v1))         ⇒ Multiple(v1, v2 :: vs)
      case (Multiple(v1, vs1), Multiple(v2, vs2)) ⇒ Multiple(v1, (v2 :: vs1) ::: vs2)
    }
  }
}

sealed trait ArbitraryGroupResult[-A, K <: GroupingCriteria, +T] {
  def key: Key[K#Key]
  def totals: T
}

object ArbitraryGroupResult {
  case class Empty[A, K <: GroupingCriteria, T](totals: T) extends ArbitraryGroupResult[A, K, T] {
    val key: Key[K#Key] = Key.NoKey
  }
  case class ##@[A](records: List[A]) extends ArbitraryGroupResult[A, GNil, Nothing] {
    type SubGroup = this.type
    def subGroup: SubGroup = this
    def key: Key[GNil] = Key.NoKey
    def totals: Nothing = ???
  }

  case class ~::[
  A,
  KH <: ##[_, _],
  KT <: GroupingCriteria,
  T
  ](
     key: Key[KH],
     totals: T,
     subGroup: ArbitraryGroupResult[A, KT, T]
   ) extends ArbitraryGroupResult[A, KH &:: KT, T] {
    type SubGroup = ArbitraryGroupResult[A, KT, T]
  }

  case class ~**[
  A,
  K <: GroupingCriteria,
  T
  ](
     totals: T,
     recHead: ArbitraryGroupResult[A, K, T],
     recTail: NonEmptyList[ArbitraryGroupResult[A, K, T]]
   ) extends ArbitraryGroupResult[A, K, T] {
    def key: Key[K#Key] = records.map(_.key).reduce
    def records: NonEmptyList[ArbitraryGroupResult[A, K, T]] = recHead :: recTail
  }
}
