package com.datarootlabs.trembita.ql


import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import com.datarootlabs.trembita._
import ArbitraryGroupResult._
import GroupingCriteria._
import AggDecl._
import cats.implicits._
import cats.data.NonEmptyList
import QueryBuilder._


protected[trembita]
trait trembitaql[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb] {
  def apply(records: Seq[A], queryF: QueryBuilder.Empty[A] ⇒ Query[A, G, T, R, Comb])
  : ArbitraryGroupResult[A, G, AggFunc.Result[T, R, Comb]]
}

object trembitaql {
  def impl[
  A: c.WeakTypeTag,
  G <: GroupingCriteria : c.WeakTypeTag,
  T <: AggDecl : c.WeakTypeTag,
  R <: AggRes : c.WeakTypeTag,
  Comb: c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[trembitaql[A, G, T, R, Comb]] = {

    import c.universe._

    val G = weakTypeOf[G].dealias
    val T = weakTypeOf[T].dealias
    val R = weakTypeOf[R].dealias
    val A = weakTypeOf[A].dealias
    val Comb = weakTypeOf[Comb].dealias
    val gnil = typeOf[GNil].dealias

    @tailrec def criteriasTypes(acc: List[(Type, Type)], currType: Type): List[(Type, Type)] =
      currType.typeArgs match {
        case List(key, `gnil`) ⇒ ((key, gnil) :: acc).reverse
        case List(head, rest)  ⇒ criteriasTypes((head, rest) :: acc, rest)
      }

    val allCriterias = criteriasTypes(Nil, G)
    val offset = allCriterias.size - 1

    val Result = tq"ArbitraryGroupResult[$A, $G, AggFunc.Result[$T, $R, $Comb]]"

    def groupByRec(step: Int, criteriasLeft: List[(Type, Type)]): c.Tree = {
      val idx: Int = step - 1
      val grCurr = TermName(s"group_$idx")
      val resCurr = TermName(s"res_$idx")
      val totalsCurr = TermName(s"totals_$idx")
      criteriasLeft match {
        case (currCriteria, `gnil`) :: Nil ⇒ q"""
         val $resCurr = $grCurr.groupBy(_._1($idx)).mapValues { vs ⇒
           val totals = vs.foldLeft(aggF.empty) { case (acc, (_, a)) => aggF.add(acc, getT(a)) }
           (totals, vs.map(_._2))
         }.toList match {
           case Nil => Empty[$A, $currCriteria &:: $gnil, AggFunc.Result[$T, $R, $Comb]](aggF.extract(aggF.empty))
           case Seq((key, (totals, vs))) => ~::[$A, $currCriteria, $gnil, AggFunc.Result[$T, $R, $Comb]](
             Key.Single(key),
             aggF.extract(totals),
             ##@(vs.toList)
           )
           case multiple@((key1, (totals1, vs1)) :: (key2, (totals2, vs2)) :: rest) =>
             ~**[$A, $currCriteria &:: $gnil, AggFunc.Result[$T, $R, $Comb]](
               aggF.extract(
                 multiple.foldLeft(aggF.empty) { case (acc, gr) => aggF.combine(acc, gr._2._1) }
               ),
               ~::[$A, $currCriteria, $gnil, AggFunc.Result[$T, $R, $Comb]](Key.Single(key1), aggF.extract(totals1), ##@(vs1.toList)),
               NonEmptyList(
                 ~::[$A, $currCriteria, $gnil, AggFunc.Result[$T, $R, $Comb]](Key.Single(key2), aggF.extract(totals2), ##@(vs2.toList)),
                 rest.map { case (key, (totals, vs)) =>
                   ~::[$A, $currCriteria, $gnil, AggFunc.Result[$T, $R, $Comb]](Key.Single(key), aggF.extract(totals), ##@(vs.toList))
                 })
             )
         }
         """
        case (currGH, currGT) :: rest      ⇒
          val grNext = TermName(s"group_$step")
          val totalsNext = TermName(s"totals_$step")
          val resNext = TermName(s"res_$step")
          q"""
           val $resCurr = $grCurr.groupBy(_._1($idx)).map { case (key, $grNext) =>
             (..${groupByRec(step + 1, rest)})
             val $totalsNext = $resNext.totals
             ~::[ $A, $currGH, $currGT, AggFunc.Result[$T, $R, $Comb] ](
               Key.Single(key), $totalsNext, $resNext
             )
           }.toList match {
             case Nil => Empty[$A, $currGH &:: $currGT, AggFunc.Result[$T, $R, $Comb]](aggF.extract(aggF.empty))
             case single :: Nil => single
             case multiple@(gr1 :: gr2 :: rest) =>
               val totals = multiple.foldLeft(aggF.empty) { case (acc, gr) => aggF.combine(acc, gr.totals.combiner) }
               ~**[$A, $currGH &:: $currGT, AggFunc.Result[$T, $R, $Comb]]( aggF.extract(totals), gr1, NonEmptyList(gr2, rest))
           }
         """
      }
    }

    G.typeArgs match {
      case List(gHx, gTx) ⇒
        c.Expr[trembitaql[A, G, T, R, Comb]](q"""
          import QueryBuilder._, ArbitraryGroupResult._, cats.data.NonEmptyList
          new trembitaql[$A, $G, $T, $R, $Comb] {
            def apply(records: Seq[$A], qb: QueryBuilder.Empty[$A] ⇒ Query[$A, $G, $T, $R, $Comb])
            : ArbitraryGroupResult[$A, $G, AggFunc.Result[$T, $R, $Comb]] = {
               val query: Query[$A, $G, $T, $R, $Comb] = qb(new QueryBuilder.Empty[$A])
               import query._
               val group_0 = records.filter(filterF).map(a ⇒ getG(a) → a)
               ${groupByRec(1, allCriterias)}
               Some(res_0)
                 .filter(gr => havingF(gr.totals.result))
                 .getOrElse(
                   Empty[$A, $G, AggFunc.Result[$T, $R, $Comb]](aggF.extract(aggF.empty))
                 )
            }
          }""")
    }
  }

  implicit def deriveQuery[A, G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb]
  : trembitaql[A, G, T, R, Comb] = macro impl[A, G, T, R, Comb]
}