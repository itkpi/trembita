package com.github.trembita.ql

import scala.language.higherKinds
import cats._
import cats.implicits._
import QueryResult._
import GroupingCriteria._
import cats.data.NonEmptyList
import com.github.trembita.collections._
import shapeless._

trait monoidInstances {

  /**
    * [[Monoid]] for some [[AggFunc.Result]] of [[A]]
    * producing [[R]] with combiner [[Comb]]
    *
    * @param AggF - aggregation function for the following types
    **/
  implicit def aggResMonoid[A, R, Comb](
    implicit AggF: AggFunc[A, R, Comb]
  ): Monoid[AggFunc.Result[A, R, Comb]] =
    new Monoid[AggFunc.Result[A, R, Comb]] {
      def empty: AggFunc.Result[A, R, Comb] = AggF.extract(AggF.empty)
      def combine(x: AggFunc.Result[A, R, Comb],
                  y: AggFunc.Result[A, R, Comb]): AggFunc.Result[A, R, Comb] =
        AggF.extract(AggF.combine(x.combiner, y.combiner))
    }

  /** Same to [[List]] [[Monoid]] */
  implicit def `##@-Monoid`[A]: Monoid[##@[A]] = new Monoid[##@[A]] {
    override def empty: ##@[A] = ##@(Nil)
    override def combine(x: ##@[A], y: ##@[A]): ##@[A] =
      ##@(x.records ++ y.records)
  }

  /** A workaround for [[Monoid]] invariance */
  implicit def `##@-Monoid2`[A, T]: Monoid[QueryResult[A, GNil, T]] =
    `##@-Monoid`[A].asInstanceOf[Monoid[QueryResult[A, GNil, T]]]

  /**
    * Monoid for some [[QueryResult]]
    * with records [[A]]
    * grouped by [[GH]] & [[GT]]
    * with aggregations [[T]]
    *
    * @param subResMonoid - monoid for sub query result
    * @param tMonoid      - [[Monoid]] for [[T]]
    **/
  implicit def QueryResultMonoid[A, GH <: :@[_, _], GT <: GroupingCriteria, T](
    implicit subResMonoid: Monoid[QueryResult[A, GT, T]],
    tMonoid: Monoid[T]
  ): Monoid[QueryResult[A, GH &:: GT, T]] =
    new Monoid[QueryResult[A, GH &:: GT, T]] {

      private def add[N <: Nat](
        xmul: ~**[A, GH &:: GT, T],
        ycons: ~::[A, GH, GT, T]
      ): QueryResult[A, GH &:: GT, T] =
        (ycons :: xmul.records).toList.reduce(combineImpl)

      def combineMuls(x: ~**[A, GH &:: GT, T],
                      y: ~**[A, GH &:: GT, T]): QueryResult[A, GH &:: GT, T] = {
        val xValues = x.records.toList.map(gr => gr.key → gr).toMap
        val yValues = y.records.toList.map(gr => gr.key → gr).toMap
        val merged = xValues.mergeConcat(yValues)(combineImpl).values.toList

        merged match {
          case Nil => Empty[A, GH &:: GT, T](tMonoid.empty)
          case List(single) => single
          case scala.::(head, scala.::(next, rest)) =>
            ~**(x.totals |+| y.totals, head, NonEmptyList(next, rest))
        }
      }

      private def combineImpl(
        x: QueryResult[A, GH &:: GT, T],
        y: QueryResult[A, GH &:: GT, T]
      ): QueryResult[A, GH &:: GT, T] = (x, y) match {
        case (Empty(_), _) => y
        case (_, Empty(_)) => x
        case (xcons: ~::[A, GH, GT, T], ycons: ~::[A, GH, GT, T])
            if xcons.key == ycons.key =>
          ~::(
            xcons.key,
            x.totals |+| y.totals,
            xcons.subResult |+| ycons.subResult
          )

        case (xmul: ~**[A, GH &:: GT, T], ycons: ~::[A, GH, GT, T]) =>
          add(xmul, ycons)
        case (xcons: ~::[A, GH, GT, T], ymul: ~**[A, GH &:: GT, T]) =>
          add(ymul, xcons)
        case (xmul: ~**[A, GH &:: GT, T], ymul: ~**[A, GH &:: GT, T]) =>
          combineMuls(xmul, ymul)
        case _ =>
          ~**(x.totals |+| y.totals, x, NonEmptyList(y, Nil))
      }

      def empty: QueryResult[A, GH &:: GT, T] =
        Empty[A, GH &:: GT, T](tMonoid.empty)
      def combine(
        x: QueryResult[A, GH &:: GT, T],
        y: QueryResult[A, GH &:: GT, T]
      ): QueryResult[A, GH &:: GT, T] = combineImpl(x, y)
    }
}
