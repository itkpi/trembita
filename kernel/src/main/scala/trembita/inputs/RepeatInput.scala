package trembita.inputs

import cats.{Id, Monad}
import trembita._
import trembita.internal.{EvaluatedSource, StrictSource}
import scala.reflect.ClassTag
import scala.language.higherKinds

class RepeatInput private[trembita] () extends InputT[Id, Nothing, Sequential, RepeatInput.Props] {
  def create[A: ClassTag](props: RepeatInput.Props[A])(
      implicit F: Monad[Id]
  ): BiDataPipelineT[Id, Nothing, A, Sequential] =
    EvaluatedSource.make[Id, Nothing, A, Sequential](Vector.tabulate(props.count)(_ => props.gen()), F)
}

object RepeatInput {
  class PropsT[F[_], A] private[trembita] (private[trembita] val count: Int, private[trembita] val gen: () => F[A])
  type Props[A] = PropsT[Id, A]

  @inline def props[A](count: Int)(gen: => A): Props[A]               = propsT[Id, A](count)(gen)
  @inline def propsT[F[_], A](count: Int)(gen: => F[A]): PropsT[F, A] = new PropsT[F, A](count, () => gen)
}

private[trembita] class RepeatInputT[F[_], Er] private[trembita] (implicit ctgF: ClassTag[F[_]])
    extends InputT[F, Er, Sequential, RepeatInput.PropsT[F, ?]] {
  private implicit def factg[A: ClassTag]: ClassTag[F[A]] =
    ClassTag[F[A]](ctgF.runtimeClass)

  def create[A: ClassTag](props: RepeatInput.PropsT[F, A])(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er, A, Sequential] =
    EvaluatedSource
      .make[F, Er, F[A], Sequential](F.pure(Vector.tabulate(props.count)(_ => props.gen())), F)
      .mapMImpl[F[A], A](fa => fa)
}
