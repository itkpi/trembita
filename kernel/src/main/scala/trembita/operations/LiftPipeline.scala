package trembita.operations

import cats.MonadError
import trembita.internal.{EvaluatedSource, StrictSource}
import trembita.{BiDataPipelineT, Environment, Parallel, Sequential}
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
  def liftIterableF[A: ClassTag](fa: F[Iterable[A]])(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, A, E]
}

trait LowPriorityLiftPipeline {

  implicit def liftSequentialF[F[_], Er](
      implicit F: MonadError[F, Er]
  ): LiftPipeline[F, Er, Sequential] = new LiftPipeline[F, Er, Sequential] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): BiDataPipelineT[F, Er, A, Sequential] =
      new StrictSource[F, Er, A](F.pure(xs.toIterator), F)

    def liftIterableF[A: ClassTag](
        fa: F[Iterable[A]]
    )(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, A, Sequential] =
      new StrictSource[F, Er, A](F.map(fa)(_.toIterator), F)
  }

  implicit def liftParallelF[F[_], Er](
      implicit F: MonadError[F, Er]
  ): LiftPipeline[F, Er, Parallel] = new LiftPipeline[F, Er, Parallel] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): BiDataPipelineT[F, Er, A, Parallel] =
      new StrictSource[F, Er, A](F.pure(xs.toIterator), F).to[Parallel]

    def liftIterableF[A: ClassTag](
        fa: F[Iterable[A]]
    )(implicit F: MonadError[F, Er]): BiDataPipelineT[F, Er, A, Parallel] =
      EvaluatedSource.makePure[F, Er, A, Parallel](F.map(fa)(_.toVector.par), F)
  }
}

object LiftPipeline extends LowPriorityLiftPipeline {
  def apply[F[_], Er, E <: Environment](implicit ev: LiftPipeline[F, Er, E]): LiftPipeline[F, Er, E] = ev
}
