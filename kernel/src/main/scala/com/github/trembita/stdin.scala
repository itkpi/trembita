package com.github.trembita

import cats.Monad

import scala.language.higherKinds
import scala.reflect.ClassTag

trait InputT[F[_], E <: Environment] {
  type Props[A]

  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, E]
}

object InputT {
  type Aux[F[_], E <: Environment, P0[_]] = InputT[F, E] { type Props[A] = P0[A] }
}

trait OutputT[F[_], E <: Environment] {
  type Props[A]
  type Out[G[_], A]

  def apply[A: ClassTag](props: Props[A])(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F]): Out[F, A]
}

object OutputT {
  type AuxProps[F[_], E <: Environment, P0[_]] = OutputT[F, E] { type Props[A] = P0[A] }

  type AuxOut[F[_], E <: Environment, Out0[_[_], _]] = OutputT[F, E] { type Out[G[_], A] = Out0[G, A] }

  type Aux[F[_], E <: Environment, P0[_], Out0[_[_], _]] = OutputT[F, E] {
    type Props[A]     = P0[A]
    type Out[G[_], A] = Out0[G, A]
  }
}
