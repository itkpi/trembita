package com.datarootlabs.trembita.ql


import cats._
import shapeless._
import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import com.datarootlabs.trembita._
import ArbitraryGroupResult._
import GroupingCriteria._
import Aggregation._
import cats.implicits._
import instances._


protected[trembita]
trait arbitraryGroupBy {
  def applyImpl[
  K <: GroupingCriteria : c.WeakTypeTag,
  T <: Aggregation : c.WeakTypeTag,
  A: c.WeakTypeTag
  ](c: blackbox.Context)
   (records: c.Expr[Iterable[A]])
   (getKey: c.Expr[A ⇒ K])
   (getTotals: c.Expr[A ⇒ T]): c.Expr[ArbitraryGroupResult[A, K, T]] = {

    import c.universe._

    val K = weakTypeOf[K].dealias
    val T = weakTypeOf[T].dealias
    val A = weakTypeOf[A].dealias
    val gnil = typeOf[GNil].dealias
    val tMonoid = q"cats.Monoid[$T]"
    //    println(s"A=${A}, K=${K}, T=${T}")

    @tailrec def getAllKeysWithLast(acc: List[Type], currType: Type): (List[Type], Type) = currType.typeArgs match {
      case List(key, `gnil`) ⇒ (acc, key)
      case List(head, rest)  ⇒ getAllKeysWithLast(head :: acc, rest)
    }

    val (keyInitReversed, lastKey) = getAllKeysWithLast(Nil, K)
    val offset = keyInitReversed.size

    def exprIter(step: Int, stepsLeft: Int): c.Tree = {
      val idx: Int = step + 1
      val crN = TermName(s"criteria_$step")
      val grCurr = TermName(s"group_$step")
      val resCurr = TermName(s"res_$step")
      stepsLeft match {
        case 0 ⇒ q"""
          val $resCurr = $grCurr.groupBy(_._1($idx)).mapValues { vs ⇒
            val totals = vs.foldLeft($tMonoid.empty) { case (acc, (_, v)) ⇒ $tMonoid.combine(acc, $getTotals(v)) }
            (totals, vs.map(_._2))
          }
          """
        case _ ⇒
          val grNext = TermName(s"group_${step + 1}")
          val totalsNext = TermName(s"totals_${step + 1}")
          val resNext = TermName(s"res_${step + 1}")
          q"""
            val $resCurr = $grCurr.groupBy(_._1($idx)).mapValues { case $grNext =>
              (..${exprIter(step + 1, stepsLeft - 1)})
              val $totalsNext = $resNext.foldLeft($tMonoid.empty) { case (acc, gr) ⇒ $tMonoid.combine(acc, gr._2._1) }
              ($totalsNext, $resNext)
            }
          """
      }
    }

    //
    K.typeArgs match {
      case List(kHx, kTx) ⇒
        val result = q"""
          $records.map(a ⇒ $getKey(a) → a).groupBy(_._1(0)).mapValues { group_0 ⇒
            ${exprIter(0, offset - 1)}
            val totals_0 = res_0.foldLeft($tMonoid.empty) { case (acc, gr) ⇒ $tMonoid.combine(acc, gr._2._1) }
            (totals_0, res_0)
          }.toArbitraryGroupResult[$A, $K, $T]
        """
        //        println(result)
        c.Expr[ArbitraryGroupResult[A, K, T]](result)
    }
  }

  def pipelineImpl[
  K <: GroupingCriteria : c.WeakTypeTag,
  T <: Aggregation : c.WeakTypeTag,
  A: c.WeakTypeTag
  ](c: blackbox.Context)
   (records: c.Expr[DataPipeline[A]])
   (getKey: c.Expr[A ⇒ K])
   (getTotals: c.Expr[A ⇒ T]): c.Expr[DataPipeline[ArbitraryGroupResult[A, K, T]]] = {
    import c.universe._

    val K = weakTypeOf[K].dealias
    val T = weakTypeOf[T].dealias
    val A = weakTypeOf[A].dealias

    c.Expr[DataPipeline[ArbitraryGroupResult[A, K, T]]](q"""
        DataPipeline.from({
          val forced: Iterable[$A] = $records.force
          val result = arbitraryGroupBy[$K, $T, $A](forced)($getKey)($getTotals)
          result match {
            case ~**(recs@_*) => recs
            case _ => Seq(result)
          }
        })
      """)
  }
}

object arbitraryGroupBy extends arbitraryGroupBy {
  def apply[K <: GroupingCriteria, T <: Aggregation, A]
  (records: Iterable[A])
  (getKey: A ⇒ K)
  (getTotals: A ⇒ T): ArbitraryGroupResult[A, K, T] = macro applyImpl[K, T, A]

  def pipeline[K <: GroupingCriteria, T <: Aggregation, A]
  (records: DataPipeline[A])
  (getKey: A ⇒ K)
  (getTotals: A ⇒ T): DataPipeline[ArbitraryGroupResult[A, K, T]] = macro pipelineImpl[K, T, A]

}
