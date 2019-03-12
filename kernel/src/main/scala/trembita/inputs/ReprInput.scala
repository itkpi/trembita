package trembita.inputs

import cats.MonadError
import trembita._
import trembita.internal._
import scala.language.higherKinds
import scala.reflect.ClassTag

class ReprInput[F[_], Er, E <: Environment] extends InputT[F, Er, E, E#Repr] {
  def create[A: ClassTag](props: E#Repr[A])(
      implicit F: MonadError[F, Er]
  ): BiDataPipelineT[F, Er, A, E] = EvaluatedSource.makePure[F, Er, A, E](F.pure(props), F)
}

class ReprInputF[F[_], Er, E <: Environment] extends InputT[F, Er, E, λ[β => F[E#Repr[β]]]] {
  def create[A: ClassTag](props: F[E#Repr[A]])(
      implicit F: MonadError[F, Er]
  ): BiDataPipelineT[F, Er, A, E] = EvaluatedSource.makePure[F, Er, A, E](props, F)
}
