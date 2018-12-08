package com.github.trembita


import java.io.File

import language.experimental.macros
import scala.reflect.macros.blackbox
import akka.http.scaladsl.marshalling._
import akka.http.scaladsl.model.{HttpCharset, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.github.trembita.ql.:@
import scala.concurrent.{ExecutionContext, Future}


package object httpi {
  implicit val intMarshaller: ToEntityMarshaller[Int] = Marshaller.apply(implicit ec => i => implicitly[ToEntityMarshaller[String]].apply(i.toString))

  implicit def funcRouteWithQueryParams[A, B, U]
  (implicit asQueryParam: AsQueryParam[A], toResponse: ToEntityMarshaller[B], tagName: TagName[U])
  : Get[(A => B) :@ U] = new Get[(A => B) :@ U] {
    val routable: Routable[(A => B) :@ U] = new Routable[(A => B) :@ U] {
      def apply(f: (A => B) :@ U)(implicit ec: ExecutionContext): Route =
        path(tagName.get) {
          asQueryParam() { a =>
            complete(f.value(a))
          }
        }
    }
  }

  def funcRouteImpl[A: c.WeakTypeTag, B: c.WeakTypeTag, U: c.WeakTypeTag](c: blackbox.Context): c.Expr[Auto[(A => B) :@ U]] = {
    import c.universe._

    val A = weakTypeOf[A].dealias
    val B = weakTypeOf[B].dealias
    val U = weakTypeOf[U].dealias

    val path = q"${U.typeSymbol.name.toString}"

    val inAsRoute: Tree = (
      if (A =:= typeOf[Int]) Some(reify {IntNumber}.tree)
      else if (A =:= typeOf[Long]) Some(reify {LongNumber}.tree)
      else if (A =:= typeOf[String]) Some(reify {Remaining}.tree)
      else if (A =:= typeOf[File]) Some(reify {
        fileUpload("file")
      }.tree)
      else None) match {
      case Some(p) => q"path($path / $p) { param => complete { func.value(param) } }"
      case None    => q"path($path) { entity(as[$A]) { param => complete { func.value(param) } } }"
    }

    println(
      s"""
         tag: $U
         in  : $A
         out : $B
       """)
    val code =
      q"""
        import akka.http.scaladsl.server._
        import Directives._
        import akka.http.scaladsl.unmarshalling._
        import akka.http.scaladsl.marshalling._
        import akka.http.scaladsl._
        import scala.concurrent.ExecutionContext

         new Auto[($A => $B) :@ $U] {
         val routable = new Routable[($A => $B) :@ $U] {
          def apply(func: ($A => $B) :@ $U)(implicit ec: ExecutionContext):Route = $inAsRoute
        }}
      """

    println(code)
    c.Expr[Auto[(A => B) :@ U]](
      code
    )
  }

  implicit def funcRoute[A, B, U]: Auto[(A => B) :@ U] = macro funcRouteImpl[A, B, U]

}
