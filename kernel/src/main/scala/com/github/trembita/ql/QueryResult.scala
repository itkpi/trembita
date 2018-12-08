package com.github.trembita.ql

import GroupingCriteria._
import cats._
import cats.data.NonEmptyList
import cats.implicits._

/**
  * A sum type representing some key K
  * used as a part of grouping criterias list
  *
  * @tparam K - wrapped key
  **/
sealed trait Key[+K] {

  import Key.{Multiple, Single}

  override def equals(obj: scala.Any): Boolean = (this, obj) match {
    case (Single(k1), Single(k2: K)) => k1 == k2
    case (Multiple(kh1, kt1), Multiple(kh2: K, kt2: NonEmptyList[K])) =>
      kh1 == kh2 && kt1 == kt2
    case _ => false
  }
}
object Key {

  /** Special case  */
  sealed trait NoKey extends Key[Nothing]
  case object NoKey extends NoKey

  /**
    * A single key K
    *
    * @tparam K - key type
    * @param value - key value
    **/
  case class Single[K](value: K) extends Key[K]

  /**
    * Represents a key
    * consisting of at least 2 values
    *
    * @tparam K - key type
    * @param head - first key
    * @param tail - a [[NonEmptyList]] of the rest keys
    **/
  case class Multiple[K](head: K, tail: NonEmptyList[K]) extends Key[K]

  /** [[Eq]] implementation for [[Key]] */
  implicit def keyEq[K: Eq]: Eq[Key[K]] = new Eq[Key[K]] {
    override def eqv(x: Key[K], y: Key[K]): Boolean = (x, y) match {
      case (Single(k1), Single(k2)) => k1 === k2
      case (Multiple(kh1, kt1), Multiple(kh2, kt2)) => kh1 === kh2 && kt1 === kt2
      case _ => false
    }
  }

  /** A [[Monoid]] used for keys concatenation */
  implicit def keyMonoid[K]: Monoid[Key[K]] = new Monoid[Key[K]] {
    def empty: Key[K] = NoKey
    def combine(x: Key[K], y: Key[K]): Key[K] = (x, y) match {
      case (NoKey, _) => y
      case (_, NoKey) => x
      case (Single(v1), Single(v2)) => Multiple(v1, NonEmptyList(v2, Nil))
      case (Single(v1), Multiple(v2, vs)) => Multiple(v1, v2 :: vs)
      case (Multiple(v2, vs), Single(v1)) => Multiple(v1, v2 :: vs)
      case (Multiple(v1, vs1), Multiple(v2, vs2)) =>
        Multiple(v1, (v2 :: vs1) ::: vs2)
    }
  }
}

/**
  * Sum type
  * representing a result of `query` execution
  *
  * @tparam A - record type
  * @tparam K - grouping criteria
  * @tparam T - aggregation result
  **/
sealed trait QueryResult[-A, K <: GroupingCriteria, +T] {

  /** @return - the head of grouping criterias list */
  def key: Key[K#Head]

  /** @return - aggregation result */
  def totals: T
}

object QueryResult {

  /**
    * An empty [[QueryResult]]
    *
    * @tparam A - record type
    * @tparam K - grouping criteria
    * @tparam T - aggregation result
    * @param totals - usually Monoid[T].empty
    **/
  case class Empty[A, K <: GroupingCriteria, T](totals: T)
      extends QueryResult[A, K, T] {
    val key: Key[K#Head] = Key.NoKey
  }

  /**
    * Represents just a list of records [[A]]
    *
    * @tparam A - record type
    **/
  case class ##@[A](records: List[A]) extends QueryResult[A, GNil, Nothing] {
    def key: Key[GNil] = Key.NoKey
    def totals: Nothing = throw new IllegalArgumentException("##@(...).totals")
  }

  /**
    * A cons type for [[QueryResult]]
    * containing a key [[KH]]
    * and some sub result grouped by [[KT]]
    *
    * @tparam A  - record type
    * @tparam KH - type of the first criteria
    * @tparam KT - type of the rest of the criterias
    * @tparam T  - aggregation result
    * @param key       - first criteria
    * @param totals    - rest of the criterias
    * @param subResult - [[QueryResult]] grouped by [[KT]]
    **/
  case class ~::[A, KH <: :@[_, _], KT <: GroupingCriteria, T](
    key: Key[KH],
    totals: T,
    subResult: QueryResult[A, KT, T]
  ) extends QueryResult[A, KH &:: KT, T]

  /**
    * Represents a non empty list of QueryResults
    * having at least 2 values
    * with calculated aggregations
    *
    * @tparam A - record type
    * @tparam K - grouping criteria
    * @tparam T - aggregation result
    * @param totals  - aggregation result
    * @param resHead - the first [[QueryResult]]
    * @param resTail - rest of the query results
    **/
  case class ~**[A, K <: GroupingCriteria, T](
    totals: T,
    resHead: QueryResult[A, K, T],
    resTail: NonEmptyList[QueryResult[A, K, T]]
  ) extends QueryResult[A, K, T] {
    def key: Key[K#Head] = records.map(_.key).reduce
    def records: NonEmptyList[QueryResult[A, K, T]] = resHead :: resTail
  }
}
