package trembita.outputs.internal

import cats.Monad
import trembita._
import scala.collection.generic.CanBuildFrom
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CollectionOutput[Col[x] <: Iterable[x], F[_], Er, E <: Environment] extends OutputWithPropsT[F, Er, E] {
  final type Props[A]     = CanBuildFrom[Col[Either[Er, A]], Either[Er, A], Col[Either[Er, A]]]
  final type Out[G[_], A] = G[Col[Either[Er, A]]]

  protected def intoCollection[A: ClassTag](
      repr: E#Repr[Either[Er, A]]
  )(implicit F: Monad[F], cbf: CanBuildFrom[Col[Either[Er, A]], Either[Er, A], Col[Either[Er, A]]]): F[Col[Either[Er, A]]]

  def apply[Err >: Er, A: ClassTag](props: CanBuildFrom[Col[Either[Er, A]], Either[Er, A], Col[Either[Er, A]]])(
      pipeline: BiDataPipelineT[F, Err, A, E]
  )(implicit F: Monad[F], E: E, run: E#Run[F]): F[Col[Either[Er, A]]] =
    F.flatMap(pipeline.evalRepr(E, run.asInstanceOf[E#Run[F]]))(
      repr => intoCollection[A](repr.asInstanceOf[E#Repr[Either[Er, A]]])(implicitly, F, props)
    )
}

class SequentialCollectionOutput[Col[x] <: Iterable[x], F[_], Er] extends CollectionOutput[Col, F, Er, Sequential] {
  protected def intoCollection[A: ClassTag](
      repr: Vector[Either[Er, A]]
  )(implicit F: Monad[F], cbf: CanBuildFrom[Col[Either[Er, A]], Either[Er, A], Col[Either[Er, A]]]): F[Col[Either[Er, A]]] =
    F.pure(repr.to[Col])
}

class ParallelCollectionOutput[Col[x] <: Iterable[x], F[_], Er] extends CollectionOutput[Col, F, Er, Parallel] {
  protected def intoCollection[A: ClassTag](
      repr: ParVector[Either[Er, A]]
  )(implicit F: Monad[F], cbf: CanBuildFrom[Col[Either[Er, A]], Either[Er, A], Col[Either[Er, A]]]): F[Col[Either[Er, A]]] =
    F.pure(repr.to[Col])
}
