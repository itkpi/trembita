package com.github.trembita.spark

import com.github.trembita.operations.MagnetF
import org.scalamacros.resetallattrs._

import scala.concurrent.Future
import scala.language.experimental.macros
import scala.language.higherKinds
import scala.reflect.macros.whitebox

class rewrite(val c: whitebox.Context) {
  import c.universe._

  private val Future = typeOf[Future[_]].dealias.typeConstructor
  private val Spark  = typeOf[BaseSpark].dealias
  private val executionContext =
    typeOf[scala.concurrent.ExecutionContext].dealias

  private val serializable = typeOf[java.io.Serializable]

  private val debug: Boolean = sys.env
    .get("trembita_spark_debug")
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

  def materializeFutureMagnet[A: c.WeakTypeTag, B: c.WeakTypeTag](
      f: c.Expr[A => SerializableFuture[B]]
  ): c.Tree = {
    val A       = weakTypeOf[A].dealias
    val B       = weakTypeOf[B].dealias
    val FutureB = weakTypeOf[SerializableFuture[B]].dealias
    val MagnetF = weakTypeOf[MagnetF[SerializableFuture, A, B, BaseSpark]]

    if (B.typeConstructor =:= Future) {
      c.abort(
        c.enclosingPosition,
        "Function returns nested futures which is not allowed"
      )
    }

    ifDebug {
      println("Given: ")
      println(f.tree)
    }
    val freshObjectName = TermName(c.freshName("serialization_workaround"))
    val (collectEC, initAllEc) =
      simpleTraverseForNonSerializable(
        Map.empty[String, Tree] -> List.empty[Tree]
      ) {
        case ((foundNS, exprs), nonSerializable) =>
          val name = nonSerializable.symbol.name.toString
          if (!nonSerializable.symbol.isStatic) {
            if (nonSerializable.symbol.info <:< executionContext)
              c.abort(
                c.enclosingPosition,
                s"Lambda contains reference to ExecutionContext which is not globally accessible: $nonSerializable"
              )
            else {
              c.abort(
                c.enclosingPosition,
                s"""|Lambda contains reference to non-serializable value which is not globally accessible: $nonSerializable
                    |  of type ${nonSerializable.symbol.info}
                 """.stripMargin
              )
            }
          }
          if (foundNS contains name) {
            foundNS -> exprs
          } else {
            val freshName   = TermName(c.freshName(name))
            val updatedExpr = exprs :+ q"@transient lazy val $freshName = ${c.resetAllAttrs(nonSerializable)};"
            (foundNS + (name -> q"$freshObjectName.$freshName"), updatedExpr)
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

    val freshMagnetName = TermName(c.freshName(s"MagnetMSparkFuture"))
    val magnetExpr =
      q"""
          object $freshMagnetName extends $MagnetF {
            object $freshObjectName {
              ..$initAllEc
            }
            @transient lazy val prepared: $A => $FutureB = {
              $updatedF
            }
          }
          ($freshMagnetName: $MagnetF)
      """
    ifDebug {
      println("--- Rewritten ---")
      println(magnetExpr)
    }
    c.resetAllAttrs(magnetExpr)
  }

  def materializeSerializableFuture[A: c.WeakTypeTag](fa: Expr[Future[A]]): c.Tree = {
    val A = weakTypeOf[A].dealias
    simpleTraverseForNonSerializable(
      {}
    ) {
      case (_, nonSerializable) =>
        val name = nonSerializable.symbol.name.toString
        if (!nonSerializable.symbol.isStatic) {
          if (nonSerializable.symbol.info <:< executionContext)
            c.abort(
              c.enclosingPosition,
              s"Lambda contains reference to ExecutionContext which is not globally accessible: $nonSerializable"
            )
          else {
            c.abort(
              c.enclosingPosition,
              s"""|Lambda contains reference to non-serializable value which is not globally accessible: $nonSerializable
                  |  of type ${nonSerializable.symbol.info}
                 """.stripMargin
            )
          }
        }
    }(fa.tree)

    q"""
       new FutureIsSerializable[$A] {
         def apply(): SerializableFuture.NewType[$A] = $fa.asInstanceOf[SerializableFuture.NewType[$A]]
       }
     """
  }

  private def simpleTraverseForNonSerializable[A](
      zero: A
  )(f: (A, Tree) => A): Tree => A = {
    var acc = zero
    val traverser = new Traverser {
      override def traverse(tree: c.universe.Tree): Unit =
        super.traverse(tree match {
          case EC(x) =>
            acc = f(acc, x)
            x
          case _ => tree
        })
    }
    tree =>
      {
        traverser.traverse(tree)
        acc
      }
  }

  private def simpleTransformer(replacements: String => Tree) =
    new Transformer {
      override def transform(tree: c.universe.Tree): c.universe.Tree =
        super.transform(tree match {
          case EC(x) =>
            val replacement = replacements(x.symbol.name.toString)
            replacement
          case _ => tree
        })
    }
}
