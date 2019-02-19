package trembita.operations

import cats.{Id, Monad, MonadError}
import trembita.internal.{EvaluatedSource, StrictSource}
import trembita.{BiDataPipelineT, DataPipeline, Environment, Parallel, Sequential}
import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Don't know how to lift iterables for pipeline in ${E} with ${F} context.
    In most cases ${E} requires specific implicits in scope.
    Please look up ${E} definition for more info or provide an implicit instance in scope if necessary
    """)
trait LiftPipeline[F[_], Er, E <: Environment] extends Serializable {
  def liftIterable[A: ClassTag](xs: Iterable[A]): BiDataPipelineT[F, Er, A, E]
  def liftIterableF[A: ClassTag](fa: F[Iterable[A]])(implicit F: Monad[F]): BiDataPipelineT[F, Er, A, E]
}

trait LowPriorityLiftPipeline {

  implicit def liftSequentialF[F[_], Er](
      implicit F: Monad[F]
  ): LiftPipeline[F, Er, Sequential] = new LiftPipeline[F, Er, Sequential] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): BiDataPipelineT[F, Er, A, Sequential] =
      new StrictSource[F, Er, A](F.pure(xs.toIterator), F)

    def liftIterableF[A: ClassTag](
        fa: F[Iterable[A]]
    )(implicit F: Monad[F]): BiDataPipelineT[F, Er, A, Sequential] =
      new StrictSource[F, Er, A](F.map(fa)(_.toIterator), F)
  }

  implicit def liftParallelF[F[_], Er](
      implicit F: Monad[F]
  ): LiftPipeline[F, Er, Parallel] = new LiftPipeline[F, Er, Parallel] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): BiDataPipelineT[F, Er, A, Parallel] =
      new StrictSource[F, Er, A](F.pure(xs.toIterator), F).to[Parallel]

    def liftIterableF[A: ClassTag](
        fa: F[Iterable[A]]
    )(implicit F: Monad[F]): BiDataPipelineT[F, Er, A, Parallel] =
      EvaluatedSource.make[F, Er, A, Parallel](F.map(fa)(_.toVector.par), F)
  }
}

object LiftPipeline extends LowPriorityLiftPipeline {
  def apply[F[_], Er, E <: Environment](implicit ev: LiftPipeline[F, Er, E]): LiftPipeline[F, Er, E] = ev

  implicit val liftSequential: LiftPipeline[Id, Nothing, Sequential] = new LiftPipeline[Id, Nothing, Sequential] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): DataPipeline[A, Sequential] =
      new StrictSource[Id, Nothing, A](xs.toIterator, Monad[Id])

    def liftIterableF[A: ClassTag](
        xs: Iterable[A]
    )(implicit F: Monad[Id]): DataPipeline[A, Sequential] =
      new StrictSource[Id, Nothing, A](xs.toIterator, F)
  }

  implicit val liftParallel: LiftPipeline[Id, Nothing, Parallel] = new LiftPipeline[Id, Nothing, Parallel] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): DataPipeline[A, Parallel] =
      new StrictSource[Id, Nothing, A](xs.toIterator, Monad[Id]).to[Parallel]

    def liftIterableF[A: ClassTag](
        xs: Iterable[A]
    )(implicit F: Monad[Id]): DataPipeline[A, Parallel] =
      new StrictSource[Id, Nothing, A](xs.toIterator, F).to[Parallel]
  }
}
