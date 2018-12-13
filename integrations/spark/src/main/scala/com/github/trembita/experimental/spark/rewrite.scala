package com.github.trembita.experimental.spark

import scala.language.experimental.macros
import scala.language.higherKinds
import com.github.trembita.MagnetM
import scala.annotation.StaticAnnotation
import scala.reflect.macros.whitebox
import org.scalamacros.resetallattrs._
import scala.concurrent.{ExecutionContext, Future}

class rewrite(val c: whitebox.Context) {
  import c.universe._

  private val Future = typeOf[Future[_]].dealias.typeConstructor
  private val Spark = typeOf[Spark].dealias
  private val executionContext =
    typeOf[scala.concurrent.ExecutionContext].dealias

  private def printt(any: Any): Unit = println("\t" + any)
  private def printtDeap(depth: Int)(any: Any): Unit =
    println(List.tabulate(depth)(_ => "\t").mkString + any)

  private val globalEC: c.Tree = q"scala.concurrent.ExecutionContext.global"
  private val debug: Boolean = sys.env
    .get("trembita.spark.debug")
    .flatMap(str => scala.util.Try { str.toBoolean }.toOption)
    .getOrElse(false)

  private def ifDebug[U](thunk: => U): Unit =
    if (debug) thunk

  object EC {
    def unapply(arg: Tree): Option[Tree] =
      Option(arg.symbol).collect {
        case s if s.info.resultType <:< executionContext => arg
      }
  }
  def materializeFutureImpl[A: c.WeakTypeTag, B: c.WeakTypeTag](
    f: c.Expr[A => Future[B]]
  ): c.Tree = {
    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias
    val FutureB = weakTypeOf[Future[B]].dealias

    if (B.typeConstructor =:= Future) {
      c.abort(
        c.enclosingPosition,
        "Function returns nested futures which is not allowed"
      )
    }

//    ifDebug {
    println("Given: ")
    println(f.tree)
//    }
    val (collectEC, initAllEc) =
      simpleTraverse(Map.empty[String, Tree] -> List.empty[Tree]) {
        case ((foundEC, exprs), ec) =>
          val name = ec.symbol.name.toString
          println(s"Traverse: got $name")
          if (foundEC contains name) {
            println(s"\t$name was traversed")
            foundEC -> exprs
          } else {
            val freshName = TermName(c.freshName(name))
            println(s"\tgrapping $name as $freshName")
            val updatedExpr = exprs :+ q"@transient lazy val $freshName = ${c.resetAllAttrs(ec)};"
            (foundEC + (name -> q"$freshName"), updatedExpr)
          }
      }(f.tree)
    val updatedF: c.Tree = f.tree match {
      case q"""($in) => { ..$exprs } """ =>
        val rewrittenExprs = exprs.map(simpleTransformer(collectEC).transform) // rewriteExprs(exprs)
        q"""($in) => { ..$rewrittenExprs }"""
      case other =>
        ifDebug {
          println("Other:")
          println(other)
        }
        c.abort(c.enclosingPosition, other.toString())
    }
    val magnetExpr =
      q"""
          new MagnetM[$Future, $A, $B, $Spark] {
            val prepared: $A => $FutureB = {
              ..$initAllEc
              $updatedF
            }
          }
      """
//    ifDebug {
    println("--- Rewritten ---")
    println(magnetExpr)
//    }
    c.resetAllAttrs(magnetExpr)
  }

  private def simpleTraverse[A](zero: A)(f: (A, Tree) => A): Tree => A = {
    var acc = zero
    val traverser = new Traverser {
      override def traverse(tree: c.universe.Tree): Unit =
        super.traverse(tree match {
          case EC(ec) =>
            acc = f(acc, ec)
            ec
          case _ => tree
        })
    }
    tree =>
      {
        traverser.traverse(tree)
        acc
      }
  }

  private def simpleTransformer(replacementEC: String => Tree) =
    new Transformer {
      override def transform(tree: c.universe.Tree): c.universe.Tree =
        super.transform(tree match {
          case EC(ec) =>
            println("name: " + ec.symbol.name)
            println("\tvalue: " + ec)
            val replacement = replacementEC(ec.symbol.name.toString)
            println("\treplacement: " + replacement)
            replacement
          case _ => tree
        })
    }
}
