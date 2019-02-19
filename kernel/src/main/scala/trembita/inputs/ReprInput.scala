package trembita.inputs

import cats.{Id, Monad}
import trembita._
import trembita.internal._
import scala.reflect.ClassTag
import scala.language.higherKinds

class ReprInput[E <: Environment] extends InputT[Id, Nothing, E, E#Repr] {
  def create[A: ClassTag](props: E#Repr[A])(
      implicit F: Monad[Id]
  ): DataPipeline[A, E] = EvaluatedSource.make[Id, Nothing, A, E](props, F)
}

class ReprInputF[F[_], Er, E <: Environment] extends InputT[F, Er, E, λ[β => F[E#Repr[β]]]] {
  def create[A: ClassTag](props: F[E#Repr[A]])(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er, A, E] = EvaluatedSource.make[F, Er, A, E](props, F)
}
