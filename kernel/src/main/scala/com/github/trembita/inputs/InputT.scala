package com.github.trembita.inputs

import cats.{Id, Monad}
import com.github.trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag

trait InputT[F[_], E <: Environment] {
  type Props[A]

  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, E]

  def empty[A: ClassTag](implicit F: Monad[F]): DataPipelineT[F, A, E]
}

trait LowPriorityInputs {
  implicit def sequentialInputF[F[_]]: InputT.Aux[F, Sequential, λ[α => F[Iterable[α]]]] =
    new IterableInputF[F]
}

object InputT extends LowPriorityInputs {
  type Aux[F[_], E <: Environment, P0[_]] = InputT[F, E] { type Props[A] = P0[A] }

  def apply[F[_], E <: Environment](implicit ev: InputT[F, E]): Aux[F, E, ev.Props] = ev

  implicit val sequentialInput: InputT.Aux[Id, Sequential, Iterable] =
    new IterableInput
}
