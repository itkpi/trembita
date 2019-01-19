package com.github.trembita.outputs.internal

import cats.Monad
import com.github.trembita._

import scala.collection.generic.CanBuildFrom
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CollectionOutput[Col[x] <: Iterable[x], F[_], E <: Environment] extends OutputWithPropsT[F, E] {
  final type Props[A]     = CanBuildFrom[Col[A], A, Col[A]]
  final type Out[G[_], A] = G[Col[A]]

  protected def intoCollection[A: ClassTag](repr: E#Repr[A])(implicit F: Monad[F], cbf: CanBuildFrom[Col[A], A, Col[A]]): F[Col[A]]

  def apply[A: ClassTag](props: CanBuildFrom[Col[A], A, Col[A]])(
      pipeline: DataPipelineT[F, A, E]
  )(implicit F: Monad[F], E: E, run: E#Run[F]): F[Col[A]] =
    F.flatMap(pipeline.evalRepr(E, run.asInstanceOf[E#Run[F]]))(repr => intoCollection(repr)(implicitly, F, props))
}

class SequentialCollectionOutput[Col[x] <: Iterable[x], F[_]] extends CollectionOutput[Col, F, Sequential] {
  protected def intoCollection[A: ClassTag](
      repr: Vector[A]
  )(implicit F: Monad[F], cbf: CanBuildFrom[Col[A], A, Col[A]]): F[Col[A]] = F.pure(repr.to[Col])
}

class ParallelCollectionOutput[Col[x] <: Iterable[x], F[_]] extends CollectionOutput[Col, F, Parallel] {
  protected def intoCollection[A: ClassTag](
      repr: ParVector[A]
  )(implicit F: Monad[F], cbf: CanBuildFrom[Col[A], A, Col[A]]): F[Col[A]] = F.pure(repr.to[Col])
}
