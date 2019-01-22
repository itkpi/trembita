package trembita.inputs

import cats.Monad
import trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag

trait InputT[F[_], E <: Environment, Props[_]] extends Serializable {
  def create[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, E]
}

trait InputWithEmptyT[F[_], E <: Environment] extends Serializable {
  def empty[A: ClassTag](implicit F: Monad[F]): DataPipelineT[F, A, E]
}
