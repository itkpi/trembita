package com.github.vitaliihonta.trembita.functional

import cats.{Monoid, Semigroup}
import shapeless._
import scala.collection.SortedSet
import scala.concurrent.duration.{Duration, FiniteDuration}


trait MonoidInstances {
  /** polymorphic function used in some aggregations */
  object concat extends Poly1 {
    implicit def semigroupCase[A](implicit semi: Semigroup[A]) = at[(A, A)] { case (x, y) => semi.combine(x, y) }

    implicit def valueWithVectorCase[A] = at[(A, Vector[A])] { case (x, vec) => vec :+ x }

    implicit def vectorWithValueCase[A] = at[(Vector[A], A)] { case (vec, x) => vec :+ x }

    implicit def sortedSetWithValueCase[A] = at[(SortedSet[A], A)] { case (set, x) => set + x }

    implicit def valueWithSortedSet[A] = at[(A, SortedSet[A])] { case (x, set) => set + x }
  }

  /** **********************
    * Semigroups and monoids
    * *********************/
  implicit object FiniteDurationMonoid extends Monoid[FiniteDuration] {
    override def empty: FiniteDuration = Duration.Zero

    override def combine(x: FiniteDuration, y: FiniteDuration): FiniteDuration = x + y
  }

  implicit object HNilMonoid extends Monoid[HNil] {
    override def empty: HNil = HNil
    override def combine(h1: HNil, h2: HNil): HNil = HNil
  }

  implicit def hlistMonoid[H: Monoid, T <: HList : Monoid]: Monoid[H :: T] =
    new Monoid[H :: T] {
      private val headMonoid = Monoid[H]
      private val tailMonoid = Monoid[T]

      override def empty: H :: T = headMonoid.empty :: tailMonoid.empty
      override def combine(hlist1: H :: T, hlist2: H :: T): H :: T =
        headMonoid.combine(hlist1.head, hlist2.head) :: tailMonoid.combine(hlist1.tail, hlist2.tail)
    }
}
