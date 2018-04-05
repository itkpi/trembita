package com.datarootlabs.trembita.ql


import cats._
import shapeless._

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import com.datarootlabs.trembita._
import ArbitraryGroupResult._
import GroupingCriteria._
import AggDecl._
import cats.implicits._
import instances._
import QueryBuilder._


protected[trembita]
trait trembitaql[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes] {
  def apply(records: Seq[A], queryF: Empty[A] ⇒ Query[A, G, T, R])
  : ArbitraryGroupResult[A, G, AggFunc.Result[R, Query[A, G, T, R]#Comb]]

  //
  //  def pipelineImpl[
  //  K <: GroupingCriteria : c.WeakTypeTag,
  //  T <: Aggregation : c.WeakTypeTag,
  //  A: c.WeakTypeTag
  //  ](c: blackbox.Context)
  //   (records: c.Expr[DataPipeline[A]])
  //   (getKey: c.Expr[A ⇒ K])
  //   (aggT: AggFunc[T]): c.Expr[DataPipeline[ArbitraryGroupResult[A, K, aggT.Out]]] = {
  //    import c.universe._
  //
  //    val K = weakTypeOf[K].dealias
  //    val T = weakTypeOf[T].dealias
  //    val A = weakTypeOf[A].dealias
  //
  //    c.Expr[DataPipeline[ArbitraryGroupResult[A, K, aggT.Out]]](q"""
  //        DataPipeline.from({
  //          val forced: Iterable[$A] = $records.force
  //          val result = arbitraryGroupBy[$K, $T, $A](forced)($getKey)(..$aggT)
  //          result match {
  //            case ~**(recs@_*) => recs
  //            case _ => Seq(result)
  //          }
  //        })
  //      """)
  //  }
}

object trembitaql {
  def impl[
  A: c.WeakTypeTag,
  G <: GroupingCriteria : c.WeakTypeTag,
  T <: AggDecl : c.WeakTypeTag,
  R <: AggRes : c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[trembitaql[A, G, T, R]] = {

    import c.universe._

    val G = weakTypeOf[G].dealias
    val T = weakTypeOf[T].dealias
    val R = weakTypeOf[R].dealias
    val A = weakTypeOf[A].dealias
    val gnil = typeOf[GNil].dealias

    @tailrec def criteriasCount(acc: Int, currType: Type): Int = currType.typeArgs match {
      case List(key, `gnil`) ⇒ acc + 1
      case List(head, rest)  ⇒ criteriasCount(acc + 1, rest)
    }

    val offset = criteriasCount(0, G) - 1

    def groupByRec(step: Int, stepsLeft: Int): c.Tree = {
      val idx: Int = step + 1
      val grCurr = TermName(s"group_$step")
      val resCurr = TermName(s"res_$step")
      val totalsCurr = TermName(s"totals_$step")
      stepsLeft match {
        case 0 ⇒ q"""
         val $resCurr = $grCurr.groupBy(_._1($idx)).mapValues { vs ⇒
           val totals = vs.foldLeft(aggF.empty) { case (acc, (_, a)) => aggF.add(acc, getT(a)) }
           (totals, vs.map(_._2))
         }.toSeq match {
           case Seq((key, (totals, vs))) => ~::(key, aggF.extract(totals), ##@(vs:_*))
           case multiple => ~**(
             aggF.extract(
               multiple.foldLeft(aggF.empty) { case (acc, gr) => aggF.combine(acc, gr._2._1) }
             ),
             multiple.map { case (key, (totals, vs)) =>
               ~::(key, aggF.extract(totals), ##@(vs:_*))
             }:_*
           )
         }
         """
        case _ ⇒
          val grNext = TermName(s"group_${step + 1}")
          val totalsNext = TermName(s"totals_${step + 1}")
          val resNext = TermName(s"res_${step + 1}")
          q"""
           val $resCurr = $grCurr.groupBy(_._1($idx)).map{ case (key, $grNext) =>
             (..${groupByRec(step + 1, stepsLeft - 1)})
             val $totalsNext = $resNext.totals
             ~::( key, $totalsNext, $resNext )
           }.toSeq match {
             case Seq(gr) => gr
             case multiple =>
               val totals = multiple.foldLeft(aggF.empty) { case (acc, gr) => aggF.combine(acc, gr.totals.combiner) }
               ~**( aggF.extract(totals), multiple:_* )
           }
         """
      }
    }

    G.typeArgs match {
      case List(kHx, kTx) ⇒
        c.Expr[trembitaql[A, G, T, R]](q"""
          new trembitaql[$A, $G, $T, $R] {
            def apply(records: Seq[$A], qb: Empty[$A] ⇒ Query[$A, $G, $T, $R])
            : ArbitraryGroupResult[$A, $G, AggFunc.Result[$R, Query[$A, $G, $T, $R]#Comb]] = {
               val query: Query[$A, $G, $T,$R] = qb(new Empty[$A])
               import query._
               (records.filter(filterF).map(a ⇒ getG(a) → a).groupBy(_._1(0)).flatMap { case (key, group_0) ⇒
                  ${groupByRec(0, offset - 1)}
                 if (havingF(res_0.totals.result)) Some(~::( key, res_0.totals, res_0 ))
                 else None
               }.toSeq match {
                 case Seq(gr) => gr
                 case multiple => ~**(
                   aggF.extract(
                     multiple.foldLeft(aggF.empty) { case (acc, gr) => aggF.combine(acc, gr.totals.combiner) }
                   ),
                   multiple:_*
                 )
               }).asInstanceOf[ArbitraryGroupResult[$A, $G, AggFunc.Result[$R, Query[$A, $G, $T, $R]#Comb]]]
            }
          }""")
    }
  }

  implicit def deriveQuery[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes]: trembitaql[A, G, T, R] = macro impl[A, G, T, R]
}