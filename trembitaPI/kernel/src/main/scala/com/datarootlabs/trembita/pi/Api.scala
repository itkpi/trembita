package com.github.trembita.pi


import com.github.trembita.ql.:@
import language.higherKinds
import shapeless.DepFn1
import scala.annotation.implicitNotFound


@implicitNotFound("Type ${A} is not a Function[0..22]")
trait IsFunction[A]
object IsFunction {
  implicit def functio0IsFunction[R]: IsFunction[() => R] = new IsFunction[() => R] {}
  implicit def function1IsFunction[A, B]: IsFunction[A => B] = new IsFunction[A => B] {}
  implicit def function2IsFunction[A, B, C]: IsFunction[(A, B) => C] = new IsFunction[(A, B) => C] {}
  implicit def function3IsFunction[A, B, C, D]: IsFunction[(A, B, C) => D] = new IsFunction[(A, B, C) => D] {}
  implicit def taggedIsFunction[A, U](implicit ev: IsFunction[A]): IsFunction[A :@ U] = new IsFunction[A :@ U] {}
}

/**
  * [[shapeless.HList]] like ADT
  * used for [[Function]] composition
  **/
sealed trait Api

object Api {
  /**
    * An empty [[Api]]
    * (like [[shapeless.HNil]])
    **/
  sealed trait ANil extends Api {
    def |::[H <: :@[_, _]](head: H)(implicit ev: IsFunction[H]): H |:: ANil = Api.|::(head, this)
  }
  /** A single instance of [[ANil]] */
  case object ANil extends ANil

  /**
    * Cons type (like [[shapeless.::]])
    *
    * @tparam PH - head of API (is a [[Function]])
    * @tparam PT - tail of API (actually [[ANil]] or another [[|::]])
    * @param headFunc - head function
    * @param rest     - rest of API functions
    **/
  case class |::[PH <: :@[_, _], PT <: Api](headFunc: PH, rest: PT)(implicit ev: IsFunction[PH]) extends Api

  case class +|+[P1 <: Api, P2 <: Api](first: P1, second: P2) extends Api {
    def swap: P2 +|+ P1 = Api.+|+(second, first)
  }
  /**
    * Prove that function with tag `U` is a part of platform api `A`
    *
    * @tparam U - tag
    * @tparam A - api
    **/
  @implicitNotFound("Function with tag ${U} is not a part of ${A}")
  trait IsPartOf[U, A <: Api] extends DepFn1[A] with Serializable
  object IsPartOf {
    def apply[U, A <: Api](implicit params: IsPartOf[U, A]): Aux[U, A, params.Out] = params
    type Aux[U, A <: Api, Out0] = IsPartOf[U, A] {type Out = Out0}

    /**
      * It is obvious that function [[F]] is part of api `H ?:: T`
      * where H == [[F]]
      **/
    implicit def head[F, U, T <: Api](implicit ev: IsFunction[F]): Aux[U, (F :@ U) |:: T, F] =
      new IsPartOf[U, (F :@ U) |:: T] {
        type Out = F
        def apply(l: (F :@ U) |:: T): Out = l.headFunc.value
      }

    /**
      * proof search continues in the tail
      **/
    implicit def tail[H <: :@[_, _], U, T <: Api, AtOut]
    (implicit att: IsPartOf.Aux[U, T, AtOut], ev: IsFunction[H]): Aux[U, H |:: T, AtOut] =
      new IsPartOf[U, H |:: T] {
        type Out = AtOut
        def apply(l: H |:: T): Out = att(l.rest)
      }

    implicit def firstPart[U, T <: Api, T2 <: Api, AtOut]
    (implicit att: IsPartOf.Aux[U, T, AtOut]): Aux[U, T +|+ T2, AtOut] =
      new IsPartOf[U, T +|+ T2] {
        type Out = AtOut
        def apply(t: T +|+ T2): AtOut = att(t.first)
      }

    implicit def secondPart[U, T <: Api, T2 <: Api, AtOut]
    (implicit att: IsPartOf.Aux[U, T2, AtOut]): Aux[U, T +|+ T2, AtOut] =
      new IsPartOf[U, T +|+ T2] {
        type Out = AtOut
        def apply(t: T +|+ T2): AtOut = att(t.second)
      }
  }
}
