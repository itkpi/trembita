package com.github.trembita
import cats.Monad
import com.github.trembita.internal.StrictSource

import scala.language.higherKinds
import scala.reflect.ClassTag

trait LiftPipeline[F[_], Ex <: Execution] {
  def liftIterable[A: ClassTag](xs: Iterable[A]): DataPipelineT[F, A, Ex]
  def liftIterableF[A: ClassTag](fa: F[Iterable[A]]): DataPipelineT[F, A, Ex]
}

object LiftPipeline {
  implicit def liftSequential[F[_]](
    implicit F: Monad[F]
  ): LiftPipeline[F, Sequential] = new LiftPipeline[F, Sequential] {
    def liftIterable[A: ClassTag](
      xs: Iterable[A]
    ): DataPipelineT[F, A, Sequential] =
      new StrictSource[F, A](F.pure(xs.toIterator), F)

    def liftIterableF[A: ClassTag](
      fa: F[Iterable[A]]
    ): DataPipelineT[F, A, Sequential] =
      new StrictSource[F, A](F.map(fa)(_.toIterator), F)
  }

  implicit def liftParallel[F[_]](
    implicit F: Monad[F]
  ): LiftPipeline[F, Parallel] = new LiftPipeline[F, Parallel] {
    def liftIterable[A: ClassTag](
      xs: Iterable[A]
    ): DataPipelineT[F, A, Parallel] =
      new StrictSource[F, A](F.pure(xs.toIterator), F).to[Parallel]

    def liftIterableF[A: ClassTag](
      fa: F[Iterable[A]]
    ): DataPipelineT[F, A, Parallel] =
      new StrictSource[F, A](F.map(fa)(_.toIterator), F).to[Parallel]
  }
}
