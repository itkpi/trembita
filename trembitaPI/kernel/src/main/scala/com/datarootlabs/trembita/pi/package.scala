package com.datarootlabs.trembita

import cats.Id

import language.{higherKinds, implicitConversions}
import com.datarootlabs.trembita.ql.:@
import pi.Api._

package object pi {
  implicit class ApiSyntax[A <: Api](val self: A) extends AnyVal {
    def |::[H <: :@[_, _]](head: H)(implicit ev: IsFunction[H]): H |:: A = Api.|::(head, self)
    def +|+[B <: Api](that: B): A +|+ B = Api.+|+(self, that)

    def exec[U](implicit proof: U IsPartOf A): proof.Out = proof(self)

    @inline def apply[U](implicit proof: U IsPartOf A): proof.Out = exec
  }


  trait >->[A, B] {
    def apply(a: A): B
  }

  implicit def idOut[A]: A >-> Id[A] = (a: A) => a

  implicit def interpret[A, B](a: A)(implicit f: A >-> B): B = f(a)
}
