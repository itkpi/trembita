package com.github.trembita.experimental.spark

import scala.language.experimental.macros
import scala.language.higherKinds
import com.github.trembita.MagnetM

import scala.annotation.StaticAnnotation
import scala.reflect.macros.whitebox
import scala.concurrent.{ExecutionContext, Future}


object SparkMagnet {
  def make[F[_], A, B](f: A => F[B]): MagnetM[F, A, B, Spark] = new MagnetM[F, A, B, Spark] {
    private val _f = f
    def prepared: A => F[B] = _f
  }
}

class rewrite(val c: whitebox.Context) {
  import c.universe._

  private val Future = typeOf[Future[_]].dealias.typeConstructor
  private val executionContext = typeOf[scala.concurrent.ExecutionContext].dealias

  object EC {
    def unapply(arg: Tree): Option[Tree] =
      if (arg.symbol.info.resultType =:= executionContext) Some(arg)
      else None
  }
  def materializeFutureImpl[A: c.WeakTypeTag, B: c.WeakTypeTag](f: c.Expr[A => Future[B]]): c.Expr[MagnetM[Future, A, B, Spark]] = {
    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias

    if (B.typeConstructor =:= Future) {
      c.abort(c.enclosingPosition, "Function returns nested futures which is not allowed")
    }

//    println("Given: ")
//    println(f.tree)
    val updatedF: c.Tree = f.tree match {
      case q"""($in) => { ..$exprs } """ =>
        rewriteExprs(exprs)
      case other =>
        println("Other:")
        c.abort(c.enclosingPosition, other.toString())
    }
//    println("")
//    println("Rewritten: ")
//    println(updatedF)
//    println("")
    val magnetExpr =
      q"""SparkMagnet.make[$Future, $A, $B] {
         $updatedF
      }"""
//    println("Done")
//    println(magnetExpr)
    c.Expr[MagnetM[Future, A, B, Spark]](c.untypecheck(magnetExpr))
  }

  def rewriteExprs(exprs: List[Tree]): Tree = {
    c.abort(c.enclosingPosition, "Not implemented yet")
  }
}