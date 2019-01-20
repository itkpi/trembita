package com.github.trembita.inputs

import cats.{Id, Monad}
import com.github.trembita._
import com.github.trembita.internal._
import scala.reflect.ClassTag
import scala.language.higherKinds

class ReprInput[E <: Environment] extends InputT[Id, E, E#Repr] {
  def create[A: ClassTag](props: E#Repr[A])(
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, E] = EvaluatedSource.make[Id, A, E](props, F)
}

class ReprInputF[F[_], E <: Environment] extends InputT[F, E, λ[β => F[E#Repr[β]]]] {
  def create[A: ClassTag](props: F[E#Repr[A]])(
      implicit F: Monad[F]
  ): DataPipelineT[F, A, E] = EvaluatedSource.make[F, A, E](props, F)
}
