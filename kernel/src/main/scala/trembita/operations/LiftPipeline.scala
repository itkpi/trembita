package trembita.operations

import cats.Monad
import trembita.internal.{EvaluatedSource, StrictSource}
import trembita.{DataPipelineT, Environment, Parallel, Sequential}
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Don't know how to lift iterables for pipeline in ${E} with ${F} context.
    In most cases ${E} requires specific implicits in scope.
    Please look up ${E} definition for more info or provide an implicit instance in scope if necessary
    """)
trait LiftPipeline[F[_], E <: Environment] extends Serializable {
  def liftIterable[A: ClassTag](xs: Iterable[A]): DataPipelineT[F, A, E]
  def liftIterableF[A: ClassTag](fa: F[Iterable[A]]): DataPipelineT[F, A, E]
}

object LiftPipeline {
  def apply[F[_], E <: Environment](implicit ev: LiftPipeline[F, E]): LiftPipeline[F, E] = ev

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
      liftIterableF(F.pure(xs))

    def liftIterableF[A: ClassTag](
        fa: F[Iterable[A]]
    ): DataPipelineT[F, A, Parallel] =
      EvaluatedSource.make[F, A, Parallel](F.map(fa)(xs => ParVector(xs.toVector: _*)), F)
  }
}
