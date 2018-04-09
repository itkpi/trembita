package com.datarootlabs.trembita.ql

import scala.language.higherKinds
import cats._
import cats.implicits._
import ArbitraryGroupResult._
import GroupingCriteria._
import cats.data.NonEmptyList
import com.datarootlabs.trembita.utils._


trait monoidInstances {
  implicit def aggResMonoid[A, R, Comb](implicit AggF: AggFunc[A, R, Comb]): Monoid[AggFunc.Result[A, R, Comb]] =
    new Monoid[AggFunc.Result[A, R, Comb]] {
      def empty: AggFunc.Result[A, R, Comb] = AggF.extract(AggF.empty)
      def combine(x: AggFunc.Result[A, R, Comb],
                  y: AggFunc.Result[A, R, Comb]): AggFunc.Result[A, R, Comb] =
        AggF.extract(AggF.combine(
          x.combiner,
          y.combiner
        ))
    }

  implicit def `~**-Semi`[A, G <: GroupingCriteria, T]
  (implicit grSemi: Semigroup[ArbitraryGroupResult[A, G, T]], tMonoid: Monoid[T]): Semigroup[~**[A, G, T]] =
    new Semigroup[~**[A, G, T]] {
      def combine(
                   x: ~**[A, G, T],
                   y: ~**[A, G, T]
                 ): ~**[A, G, T] = {
        val xValues = x.records.toList.groupBy(_.key)
        val yValues = y.records.toList.groupBy(_.key)
        val merged = NonEmptyList.fromListUnsafe(
          xValues.mergeConcat(yValues)(_ ::: _)
            .mapValues(_.reduce(grSemi.combine))
            .values.toList)

        ~**(tMonoid.combine(x.totals, y.totals), merged.head, NonEmptyList.fromListUnsafe(merged.tail))
      }
    }

  implicit def `##@-Monoid`[A]: Monoid[##@[A]] = new Monoid[##@[A]] {
    override def empty: ##@[A] = ##@(Nil)
    override def combine(x: ##@[A], y: ##@[A]): ##@[A] = ##@(x.records ++ y.records)
  }

  implicit def `##@-Monoid2`[A, T]: Monoid[ArbitraryGroupResult[A, GNil, T]] =
    `##@-Monoid`[A].asInstanceOf[Monoid[ArbitraryGroupResult[A, GNil, T]]]

  implicit def ArbGroupResultMonoid[
  A,
  GH <: ##[_, _],
  GT <: GroupingCriteria,
  T](implicit subGroupMonoid: Monoid[ArbitraryGroupResult[A, GT, T]],
     tMonoid: Monoid[T]): Monoid[ArbitraryGroupResult[A, GH &:: GT, T]] = new Monoid[ArbitraryGroupResult[A, GH &:: GT, T]] {

    private val mulSemi: Semigroup[~**[A, GH &:: GT, T]] = `~**-Semi`[A, GH &:: GT, T](this, tMonoid)

    private def add(xmul: ~**[A, GH &:: GT, T], ycons: ~::[A, GH, GT, T]): ~**[A, GH &:: GT, T] = {
      val newTotals = tMonoid.combine(xmul.totals, ycons.totals)
      val merged = NonEmptyList.fromListUnsafe(
        (ycons :: xmul.records).toList.groupBy(_.key)
          .mapValues(_.reduce(combine))
          .values.toList)

      ~**(newTotals, merged.head, NonEmptyList.fromListUnsafe(merged.tail))
    }

    def empty: ArbitraryGroupResult[A, GH &:: GT, T] = Empty[A, GH &:: GT, T](tMonoid.empty)
    def combine(x: ArbitraryGroupResult[A, GH &:: GT, T],
                y: ArbitraryGroupResult[A, GH &:: GT, T]): ArbitraryGroupResult[A, GH &:: GT, T] =
      (x, y) match {
        case (Empty(_), _)                                                                  ⇒ y
        case (_, Empty(_))                                                                  ⇒ x
        case (xcons: ~::[A, GH, GT, T], ycons: ~::[A, GH, GT, T]) if xcons.key == ycons.key ⇒
          ~::(xcons.key, tMonoid.combine(x.totals, y.totals), subGroupMonoid.combine(xcons.subGroup, ycons.subGroup))

        case (xmul: ~**[A, GH &:: GT, T], ycons: ~::[A, GH, GT, T])   ⇒ add(xmul, ycons)
        case (xcons: ~::[A, GH, GT, T], ymul: ~**[A, GH &:: GT, T])   ⇒ add(ymul, xcons)
        case (xmul: ~**[A, GH &:: GT, T], ymul: ~**[A, GH &:: GT, T]) ⇒ mulSemi.combine(xmul, ymul)
        case _                                                        ⇒ ~**(tMonoid.combine(x.totals, y.totals), x, NonEmptyList(y, Nil))
      }
  }
}
