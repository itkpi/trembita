package trembita.ql

import language.experimental.macros
import scala.reflect.macros.blackbox
import cats.Show
import cats.implicits._
import GroupingCriteria._
import AggRes._

object show {
  implicit def showTagged[A, U]: Show[A :@ U] = macro showTaggedImpl[A, U]

  implicit def showAggFuncResult[In, A: Show, Comb]: Show[AggFunc.Result[In, A, Comb]] =
    new Show[AggFunc.Result[In, A, Comb]] {
      override def show(t: AggFunc.Result[In, A, Comb]): String = t.result.show
    }

  implicit object ShowGNil extends Show[GNil] {
    def show(t: GNil): String = "∅"
  }

  implicit def showGroupCriteriaCons[GH <: :@[_, _]: Show, GT <: GroupingCriteria: Show]: Show[GH &:: GT] = new Show[GH &:: GT] {
    override def show(t: GH &:: GT): String = {
      val tailStr = t.tail match {
        case GNil  => ""
        case other => s" & ${other.show}"
      }
      s"${t.head.show}$tailStr"
    }
  }

  implicit object ShowRNil extends Show[RNil] {
    override def show(t: RNil): String = "∅"
  }

  implicit def showAggRes[AgH <: :@[_, _]: Show, AgT <: AggRes: Show]: Show[AgH *:: AgT] = new Show[AgH *:: AgT] {
    override def show(t: AgH *:: AgT): String = {
      val tailStr = t.tail match {
        case RNil  => ""
        case other => s" & ${other.show}"
      }
      s"${t.head.show} $tailStr"
    }
  }

  def showTaggedImpl[A: c.WeakTypeTag, U: c.WeakTypeTag](
      c: blackbox.Context
  ): c.Expr[Show[A :@ U]] = {
    import c.universe._
    val A     = weakTypeOf[A].dealias
    val u     = weakTypeOf[U]
    val U     = u.dealias
    val uName = u.typeSymbol.toString.dropWhile(_ != ' ').tail
    c.Expr[Show[A :@ U]](q"""
      new cats.Show[$A :@ $U]{
        def show(v: $A :@ $U): String = $uName + ": " + v.value
      }
    """)
  }
}
