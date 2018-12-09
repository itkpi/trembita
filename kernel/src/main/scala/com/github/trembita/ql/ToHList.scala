package com.github.trembita.ql

import com.github.trembita.ql.AggRes.{*::, RNil}
import com.github.trembita.ql.GroupingCriteria.{&::, GNil}
import shapeless._
import scala.language.experimental.macros

trait ToHList[A] extends DepFn1[A] { type Out }

object ToHList {
  type Aux[A, Out0] = ToHList[A] { type Out = Out0 }
  def apply[A](implicit ev: ToHList[A]): Aux[A, ev.Out] = ev

  implicit val gnilToHList: ToHList.Aux[GNil, HNil] = new ToHList[GNil] {
    type Out = HNil
    def apply(t: GNil): HNil = HNil
  }

  implicit def gconsToHList[A, U, RT <: GroupingCriteria, L <: HList](
    implicit ev: ToHList.Aux[RT, L]
  ): ToHList.Aux[(A :@ U) &:: RT, A :: L] =
    new ToHList[(A :@ U) &:: RT] {
      type Out = A :: L
      def apply(t: (A :@ U) &:: RT): Out = t.head.value :: ev(t.tail)
    }

  implicit def aggFuncResToHList[In, R, Comb](
    implicit ev: ToHList[R]
  ): ToHList.Aux[AggFunc.Result[In, R, Comb], ev.Out] =
    new ToHList[AggFunc.Result[In, R, Comb]] {
      type Out = ev.Out
      def apply(t: AggFunc.Result[In, R, Comb]): Out = ev(t.result)
    }

  implicit val rnilToHList: ToHList.Aux[RNil, HNil] = new ToHList[RNil] {
    type Out = HNil
    def apply(t: RNil): HNil = HNil
  }

  implicit def rconsToHList[A, U, RT <: AggRes, L <: HList](
    implicit ev: ToHList.Aux[RT, L]
  ): ToHList.Aux[(A :@ U) *:: RT, A :: L] =
    new ToHList[(A :@ U) *:: RT] {
      type Out = A :: L
      def apply(t: (A :@ U) *:: RT): Out = t.head.value :: ev(t.tail)
    }

  implicit def recordsToHList[A, B]
    : ToHList.Aux[QueryResult[A, GNil, B], List[A]] =
    new ToHList[QueryResult[A, GNil, B]] {
      type Out = List[A]
      def apply(t: QueryResult[A, GNil, B]): List[A] = t match {
        case recs: QueryResult.##@[A]      => recs.records
        case e: QueryResult.Empty[A, _, _] => Nil
        case _ =>
          throw new NotImplementedError(
            "Impossible case for trembita-constructed QueryResult"
          )
      }
    }

  implicit def consToHList0[A, K, U, T](
    implicit subQueryToHList: ToHList[QueryResult[A, GNil, T]],
    totalsToHList: ToHList[T]
  ): ToHList.Aux[QueryResult[A, (K :@ U) &:: GNil, T],
                 totalsToHList.Out :: List[
                   K :: totalsToHList.Out :: subQueryToHList.Out :: HNil
                 ] :: HNil] =
    new ToHList[QueryResult[A, (K :@ U) &:: GNil, T]] { self =>
      type Out =
        totalsToHList.Out :: List[
          K :: totalsToHList.Out :: subQueryToHList.Out :: HNil
        ] :: HNil
      override def apply(t: QueryResult[A, (K :@ U) &:: GNil, T]): Out =
        t match {
          case t: QueryResult.~::[A, K :@ U, GNil, T] =>
            val totals = totalsToHList(t.totals)
            totals :: List(
              t.key.values.head.value :: totals :: subQueryToHList(t.subResult) :: HNil
            ) :: HNil

          case t: QueryResult.~**[A, (K :@ U) &:: GNil, T] =>
            val totals = totalsToHList(t.totals)
            totals :: t.records.toList.flatMap {
              case tt: QueryResult.~::[A, K :@ U, GNil, T] =>
                Some(
                  tt.key.values.head.value :: totalsToHList(tt.totals) :: subQueryToHList(
                    tt.subResult
                  ) :: HNil
                )

              case e: QueryResult.Empty[A, (K :@ U) &:: GNil, T] =>
                None

              case _ =>
                throw new NotImplementedError(
                  "Impossible case for trembita-constructed QueryResult"
                )
            } :: HNil

          case e: QueryResult.Empty[A, (K :@ U) &:: GNil, T] =>
            val totals = totalsToHList(e.totals)
            totals :: Nil :: HNil
        }
    }

  implicit def consToHList[A, K, U, KT <: GroupingCriteria, T](
    implicit subQueryToHList: ToHList[QueryResult[A, KT, T]],
    totalsToHList: ToHList[T],
    ev: KT =:!= GNil
  ): ToHList.Aux[QueryResult[A, (K :@ U) &:: KT, T], totalsToHList.Out :: List[
    K :: totalsToHList.Out :: List[subQueryToHList.Out] :: HNil
  ] :: HNil] =
    new ToHList[QueryResult[A, (K :@ U) &:: KT, T]] { self =>
      type Out =
        totalsToHList.Out :: List[
          K :: totalsToHList.Out :: List[subQueryToHList.Out] :: HNil
        ] :: HNil
      override def apply(t: QueryResult[A, (K :@ U) &:: KT, T]): Out = t match {
        case t: QueryResult.~::[A, K :@ U, KT, T] =>
          val totals = totalsToHList(t.totals)
          totals :: List(
            t.key.values.head.value :: totals :: List(
              subQueryToHList(t.subResult)
            ) :: HNil
          ) :: HNil

        case t: QueryResult.~**[A, (K :@ U) &:: KT, T] =>
          val totals = totalsToHList(t.totals)
          totals :: t.records.toList.flatMap {
            case tt: QueryResult.~::[A, K :@ U, KT, T] =>
              Some(
                tt.key.values.head.value :: totalsToHList(tt.totals) :: List(
                  subQueryToHList(tt.subResult)
                ) :: HNil
              )

            case e: QueryResult.Empty[A, (K :@ U) &:: KT, T] =>
              None

            case _ =>
              throw new NotImplementedError(
                "Impossible case for trembita-constructed QueryResult"
              )
          } :: HNil

        case e: QueryResult.Empty[A, (K :@ U) &:: KT, T] =>
          totalsToHList(e.totals) :: Nil :: HNil
      }
    }
}
