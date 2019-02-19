package trembita.inputs

import cats.Monad
import trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag

trait InputT[F[_],Er, E <: Environment, Props[_]] extends Serializable {
  def create[A: ClassTag](props: Props[A])(implicit F: Monad[F]): BiDataPipelineT[F,Er, A, E]
}

trait InputWithEmptyT[F[_],Er, E <: Environment] extends Serializable {
  def empty[A: ClassTag](implicit F: Monad[F]): BiDataPipelineT[F,Er, A, E]
}
