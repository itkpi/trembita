package com.github.trembita.inputs

import cats.Monad
import com.github.trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag

trait InputT[F[_], E <: Environment, Props[_]] {
  def create[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, E]
}

trait InputWithEmptyT[F[_], E <: Environment] {
  def empty[A: ClassTag](implicit F: Monad[F]): DataPipelineT[F, A, E]
}
