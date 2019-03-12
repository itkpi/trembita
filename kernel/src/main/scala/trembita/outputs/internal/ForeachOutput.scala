package trembita.outputs.internal

import cats.MonadError
import trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag

class ForeachOutput[F[_], Er, A, E <: Environment](f: A => Unit) extends OutputT[F, Er, A, E] {
  type Props[x]     = x => Unit
  type Out[G[_], x] = G[Unit]

  val props: A => Unit = f

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Unit] =
    F.map(pipeline.evalRepr)(
      repr =>
        E.FlatMapRepr.foreach[Either[Er, AA]](repr.asInstanceOf[E.Repr[Either[Er, AA]]]) {
          case _: Left[_, _] =>
          case Right(v)      => props(v.asInstanceOf[A])
      }
    )
}

class OnCompleteOutput[F[_], Er, A, E <: Environment](f: Either[Er, A] => Unit) extends OutputT[F, Er, A, E] {
  type Props[x]     = Either[Er, x] => Unit
  type Out[G[_], x] = G[Unit]

  val props: Either[Er, A] => Unit = f

  override def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Unit] = F.map(pipeline.evalRepr)(
    repr => E.FlatMapRepr.foreach[Either[Er, A]](repr.asInstanceOf[E.Repr[Either[Er, A]]])(props)
  )
}
