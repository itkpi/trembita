package trembita.outputs.internal

import cats.Monad
import trembita._

import scala.language.higherKinds
import scala.reflect.ClassTag

class ForeachOutput[F[_], A, E <: Environment](f: A => Unit) extends OutputT[F, A, E] {
  type Props[x]     = x => Unit
  type Out[G[_], x] = G[Unit]

  val props: A => Unit = f

  def apply(
      pipeline: DataPipelineT[F, A, E]
  )(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[Unit] =
    F.map(pipeline.evalRepr)(repr => E.FlatMapRepr.foreach[A](repr.asInstanceOf[E.Repr[A]])(props))
}
