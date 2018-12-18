package com.github.trembita.operations

import cats.Monad
import com.github.trembita.internal.StrictSource
import com.github.trembita.{DataPipelineT, Environment, Parallel, Sequential}
import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Don't know how to lift iterables for pipeline in ${Ex} with ${F} context.
    In most cases ${Ex} requires specific implicits in scope.
    Please look up ${Ex} definition for more info or provide an implicit instance in scope if necessary
    """)
trait LiftPipeline[F[_], Ex <: Environment] {
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
