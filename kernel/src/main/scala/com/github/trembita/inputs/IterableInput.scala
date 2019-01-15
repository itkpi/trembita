package com.github.trembita.inputs

import cats.{Id, Monad}
import com.github.trembita._
import com.github.trembita.operations.LiftPipeline
import scala.language.higherKinds
import scala.reflect.ClassTag

class IterableInput extends InputT[Id, Sequential] {
  type Props[A] = Iterable[A]
  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[Id]): DataPipeline[A, Sequential] =
    LiftPipeline[Id, Sequential].liftIterable[A](props)
}

class IterableInputF[F[_]] extends InputT[F, Sequential] {
  type Props[A] = F[Iterable[A]]
  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, Sequential] =
    LiftPipeline[F, Sequential].liftIterableF[A](props)
}

class EmptyInput extends InputT[Id, Sequential] {
  type Props[A] = Unit

  def apply[A: ClassTag](props: Unit)(
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Sequential] = LiftPipeline[Id, Sequential].liftIterable[A](Seq.empty)
}
