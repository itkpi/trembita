package com.github.trembita.inputs

import cats.{Id, Monad}
import com.github.trembita._
import com.github.trembita.operations.LiftPipeline

import scala.collection.parallel.ParIterable
import scala.language.higherKinds
import scala.reflect.ClassTag

class IterableInput extends InputT[Id, Sequential] {
  type Props[A] = Iterable[A]
  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[Id]): DataPipeline[A, Sequential] =
    LiftPipeline[Id, Sequential].liftIterable[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Sequential] = apply[A](Seq.empty)
}

class ParIterableInput extends InputT[Id, Parallel] {
  type Props[A] = Iterable[A]

  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[Id]): DataPipeline[A, Parallel] =
    LiftPipeline[Id, Parallel].liftIterable[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Parallel] = apply[A](Seq.empty)
}

class IterableInputF[F[_]] extends InputT[F, Sequential] {
  type Props[A] = F[Iterable[A]]
  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, Sequential] =
    LiftPipeline[F, Sequential].liftIterableF[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[F]
  ): DataPipelineT[F, A, Sequential] = apply[A](F.pure[Iterable[A]](Seq.empty))
}

class ParIterableInputF[F[_]] extends InputT[F, Parallel] {
  type Props[A] = F[Iterable[A]]

  def apply[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, Parallel] =
    LiftPipeline[F, Parallel].liftIterableF[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[F]
  ): DataPipelineT[F, A, Parallel] = apply[A](F.pure[Iterable[A]](Seq.empty))
}
