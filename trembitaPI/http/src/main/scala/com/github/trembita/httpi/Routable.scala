package com.github.trembita.httpi


import language.experimental.macros
import scala.reflect.macros.blackbox
import com.github.trembita.pi._
import akka.http.scaladsl.server._
import com.github.trembita.pi.Api._
import Directives._
import scala.concurrent.ExecutionContext
import java.io.File
import akka.http.scaladsl.marshalling.{ToEntityMarshaller, ToResponseMarshallable}
import akka.http.scaladsl.server.util.Tupler
import com.github.trembita.ql.:@
import shapeless._

import scala.annotation.tailrec


trait TagName[U] {def get: String}
object TagName {
  def deriveImpl[U: c.WeakTypeTag](c: blackbox.Context): c.Expr[TagName[U]] = {
    import c.universe._

    val U = weakTypeOf[U].dealias

    c.Expr[TagName[U]](
      q"""
         new TagName[$U] { val get: String = ${U.typeSymbol.name.toString} }
       """)
  }

  implicit def derive[U]: TagName[U] = macro deriveImpl[U]
}

trait AsQueryParam[A] {
  def apply(): Directive1[A]
}

object AsQueryParam {
  def macroPrimitive[A: c.WeakTypeTag, U: c.WeakTypeTag](c: blackbox.Context): c.Expr[AsQueryParam[A :@ U]] = {
    import c.universe._

    val U = weakTypeOf[U]
    val A = weakTypeOf[A].dealias
    val int = typeOf[Int].dealias
    val long = typeOf[Long].dealias
    val double = typeOf[Double].dealias
    val string = typeOf[String].dealias
    val nothing = typeOf[Nothing].dealias

    if (A =:= int || A =:= long || A =:= double || A =:= string) {
      val name = U.typeSymbol.name.toString

      if (U =:= nothing) c.abort(c.enclosingPosition, s"Tag U must not be Nothing!")
      else c.Expr[AsQueryParam[A :@ U]](
        q"""
         new AsQueryParam[$A :@ $U] {
           def apply(): Directive1[$A :@ $U] = parameter(Symbol("" + $name).as[$A])
         }
       """
      )
    } else c.abort(c.enclosingPosition, s"Only primitive type allowed, give: $A")
  }

  def macroHListImpl[
  H: c.WeakTypeTag,
  U: c.WeakTypeTag,
  T <: HList : c.WeakTypeTag,
  TU <: HList : c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[AsQueryParam[H :: T]] = {
    import c.universe._

    val H = weakTypeOf[H].dealias
    val T = weakTypeOf[T].dealias
    val U = weakTypeOf[U].dealias
    val TU = weakTypeOf[TU].dealias
    val nothing = typeOf[Nothing].dealias

    if (U =:= nothing) c.abort(c.enclosingPosition, "Head tag U must not be nothing")
    if (TU =:= nothing) c.abort(c.enclosingPosition, "Tail tags TU must no be nothing")

    val names: List[String] = {
      @tailrec def namesIter(t: Type, acc: List[String]): List[String] = t.typeArgs match {
        case Nil                => acc.reverse
        case List(headT, tailT) => namesIter(tailT, headT.typeSymbol.name.toString :: acc)
      }

      namesIter(TU, Nil)
    }

    @tailrec def genIter(headT: Type, headName: String, tailT: Type, nameIdx: Int, acc: List[(Type, String)]): c.Expr[Directive0] =
      tailT.typeArgs match {
        case Nil                => c.Expr[Directive0] {
          if (acc.isEmpty)
            q""" parameter(Symbol("" + $headName).as[$headT]) """
          else
            q"""  parameters( ${acc.map { case (t, n) => q"$n.as[$t], " }} Symbol("" + $headName).as[$headT] )  """
        }
        case List(nextT, restT) => genIter(nextT, names(nameIdx), restT, nameIdx + 1, (headT, headName) :: acc)
      }

    c.Expr[AsQueryParam[H :: T]](
      q"""
         new AsQueryParam[$H :: $T, $U :: $TU] {
           def apply(): Directive0 = ${genIter(H, U.typeSymbol.name.toString, T, 0, Nil)}
         }
       """
    )
  }

  def caseClassImpl[
  A <: Product : c.WeakTypeTag
  ](c: blackbox.Context): c.Expr[AsQueryParam[A]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val Atag = weakTypeTag[A]

    if (!Atag.tpe.typeSymbol.isClass && !Atag.tpe.typeSymbol.asClass.isCaseClass) {
      c.abort(c.enclosingPosition, s"Type $A is not a case class")
    } else {
      object CaseField {
        def unapply(trmSym: TermSymbol): Option[(Name, Type)] = {
          if (trmSym.isVal && trmSym.isCaseAccessor) Some((TermName(trmSym.name.toString.trim), trmSym.typeSignature))
          else None
        }
      }

      val parameters = {
        val values = A.decls.collect {
          case CaseField(name, tpe) => (name, tpe)
        }

        val params = values.map { case (n, t) => q"Symbol(${n.toString}).as[$t]" }
        q"parameters(..$params).tmap(${A.typeSymbol.companion}.apply _ tupled)"
      }

      val expr =
        q"""
         new AsQueryParam[$A] {
           def apply(): Directive1[$A] = $parameters
         }
       """

//      println(expr)

      c.Expr[AsQueryParam[A]](expr)
    }
  }


  implicit def intAsQueryParam[U]: AsQueryParam[Int :@ U] = macro macroPrimitive[Int, U]
  implicit def doubleAsQueryParam[U]: AsQueryParam[Double :@ U] = macro macroPrimitive[Double, U]
  implicit def longAsQueryParam[U]: AsQueryParam[Long :@ U] = macro macroPrimitive[Long, U]
  implicit def stringAsQueryParam[U]: AsQueryParam[String :@ U] = macro macroPrimitive[String, U]
  implicit def hlistAsQueryParam[H, U, T <: HList, TU <: HList]: AsQueryParam[H :: T] = macro macroHListImpl[H, U, T, TU]
  implicit def caseClass[A <: Product]: AsQueryParam[A] = macro caseClassImpl[A]

  def apply[A](implicit asQueryParam: AsQueryParam[A]): AsQueryParam[A] = asQueryParam
}

protected[trembita] trait Method[A] {def routable: Routable[A]}
protected[trembita] trait Auto[A] extends Method[A]
protected[trembita] trait Get[A] extends Method[A]
protected[trembita] trait Post[A] extends Method[A]
protected[trembita] trait Delete[A] extends Method[A]

trait Routable[A] {
  self =>

  def apply(a: A)(implicit ec: ExecutionContext): Route

  def post: Post[A] = new Post[A] {
    val routable: Routable[A] = new Routable[A] {
      def apply(a: A)(implicit ec: ExecutionContext): Route =
        Directives.post(self.apply(a))
    }
  }

  def delete: Delete[A] = new Delete[A] {
    val routable: Routable[A] = new Routable[A] {
      def apply(a: A)(implicit ec: ExecutionContext): Route =
        Directives.delete(self.apply(a))
    }
  }
}


object Routable {
  implicit def head[F <: :@[_, _]](implicit F: Routable[F], proof: IsFunction[F]): Routable[F |:: ANil] = new Routable[F |:: ANil] {
    def apply(a: F |:: ANil)(implicit ec: ExecutionContext): Route = F.apply(a.headFunc)
  }

  implicit def cons[F <: :@[_, _], T <: Api](implicit F: Routable[F], T: Routable[T]): Routable[F |:: T] = new Routable[F |:: T] {
    def apply(a: F |:: T)(implicit ec: ExecutionContext): Route = F.apply(a.headFunc) ~ T.apply(a.rest)
  }

  def apply[A](implicit R: Routable[A]): Routable[A] = R
  def auto[A](implicit R: Auto[A]): Routable[A] = R.routable
  def get[A](implicit G: Get[A]): Routable[A] = G.routable
  def post[A](implicit P: Post[A]): Routable[A] = P.routable
  def delete[A](implicit D: Delete[A]): Routable[A] = D.routable
}
