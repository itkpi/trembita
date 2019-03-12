package trembita.outputs.internal

import cats.MonadError
import trembita._
import scala.collection.generic.CanBuildFrom
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CollectionOutput[Col[x] <: Iterable[x], F[_], Er, E <: Environment] extends OutputWithPropsT[F, Er, E] {
  final type Props[A]     = CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]]
  final type Out[G[_], A] = G[Col[Either[Er, A]]]

  protected def intoCollection[A: ClassTag](
      repr: E#Repr[Either[Er, A]]
  )(implicit F: MonadError[F, Er], cbf: CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]]): F[Col[Either[Er, A]]]

  def apply[Err >: Er, A: ClassTag](props: CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]])(
      pipeline: BiDataPipelineT[F, Err, A, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F]): F[Col[Either[Er, A]]] =
    F.flatMap(pipeline.evalRepr(E, run.asInstanceOf[E#Run[F]]))(
      repr => intoCollection[A](repr.asInstanceOf[E#Repr[Either[Er, A]]])(implicitly, F, props)
    )
}

class SequentialCollectionOutput[Col[x] <: Iterable[x], F[_], Er] extends CollectionOutput[Col, F, Er, Sequential] {
  protected def intoCollection[A: ClassTag](
      repr: Vector[Either[Er, A]]
  )(implicit F: MonadError[F, Er], cbf: CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]]): F[Col[Either[Er, A]]] =
    F.pure(repr.to[Col])
}

class ParallelCollectionOutput[Col[x] <: Iterable[x], F[_], Er] extends CollectionOutput[Col, F, Er, Parallel] {
  protected def intoCollection[A: ClassTag](
      repr: ParVector[Either[Er, A]]
  )(implicit F: MonadError[F, Er], cbf: CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]]): F[Col[Either[Er, A]]] =
    F.pure(repr.to[Col])
}

trait CollectionIgnoreErrorsOutput[Col[x] <: Iterable[x], F[_], Er, E <: Environment] extends OutputWithPropsT[F, Er, E] {
  final type Props[A]     = CanBuildFrom[Nothing, A, Col[A]]
  final type Out[G[_], A] = G[Col[A]]

  protected def intoCollection[A: ClassTag](
      repr: E#Repr[Either[Er, A]]
  )(implicit F: MonadError[F, Er], cbf: CanBuildFrom[Nothing, A, Col[A]]): F[Col[A]]

  def apply[Err >: Er, A: ClassTag](props: CanBuildFrom[Nothing, A, Col[A]])(
      pipeline: BiDataPipelineT[F, Err, A, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F]): F[Col[A]] =
    F.flatMap(pipeline.evalRepr(E, run.asInstanceOf[E#Run[F]]))(
      repr => intoCollection[A](repr.asInstanceOf[E#Repr[Either[Er, A]]])(implicitly, F, props)
    )
}

class SequentialIgnoreErrorsCollectionOutput[Col[x] <: Iterable[x], F[_], Er] extends CollectionIgnoreErrorsOutput[Col, F, Er, Sequential] {
  protected def intoCollection[A: ClassTag](
      repr: Vector[Either[Er, A]]
  )(implicit F: MonadError[F, Er], cbf: CanBuildFrom[Nothing, A, Col[A]]): F[Col[A]] =
    F.pure(
      repr.view
        .collect {
          case Right(v) => v
        }
        .to[Col]
    )
}

class ParallelIgnoreErrorsCollectionOutput[Col[x] <: Iterable[x], F[_], Er] extends CollectionIgnoreErrorsOutput[Col, F, Er, Parallel] {
  protected def intoCollection[A: ClassTag](
      repr: ParVector[Either[Er, A]]
  )(implicit F: MonadError[F, Er], cbf: CanBuildFrom[Nothing, A, Col[A]]): F[Col[A]] =
    F.pure(
      repr
        .collect {
          case Right(v) => v
        }
        .to[Col]
    )
}
