package trembita.inputs

import cats.{Id, Monad}
import trembita._
import trembita.internal.EvaluatedSource
import trembita.operations.LiftPipeline
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.Random

class IterableInput[Col[+x] <: Iterable[x]] private[trembita] (implicit cbf: CanBuildFrom[List[_], _, Col[_]])
    extends InputT[Id, Sequential, Col]
    with InputWithEmptyT[Id, Sequential] {

  def create[A: ClassTag](props: Col[A])(implicit F: Monad[Id]): DataPipeline[A, Sequential] =
    LiftPipeline[Id, Sequential].liftIterable[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Sequential] = create[A](cbf(Nil).result().asInstanceOf[Col[A]])
}

class ParIterableInput[Col[+x] <: Iterable[x]] private[trembita] (implicit cbf: CanBuildFrom[List[_], _, Col[_]])
    extends InputT[Id, Parallel, Col]
    with InputWithEmptyT[Id, Parallel] {
  def create[A: ClassTag](props: Col[A])(implicit F: Monad[Id]): DataPipeline[A, Parallel] =
    LiftPipeline[Id, Parallel].liftIterable[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Parallel] = create[A](cbf(Nil).result().asInstanceOf[Col[A]])
}

class IterableInputF[F[+ _], Col[+x] <: Iterable[x]] private[trembita] (implicit cbf: CanBuildFrom[List[_], _, Col[_]])
    extends InputT[F, Sequential, λ[β => F[Col[β]]]]
    with InputWithEmptyT[F, Sequential] {

  final type Props[A] = F[Col[A]]
  def create[A: ClassTag](props: Props[A])(implicit F: Monad[F]): DataPipelineT[F, A, Sequential] =
    LiftPipeline[F, Sequential].liftIterableF[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[F]
  ): DataPipelineT[F, A, Sequential] = create[A](F.pure[Col[A]](cbf(Nil).result().asInstanceOf[Col[A]]))
}

class ParIterableInputF[F[+ _], Col[+x] <: Iterable[x]] private[trembita] (implicit cbf: CanBuildFrom[List[_], _, Col[_]])
    extends InputT[F, Parallel, λ[β => F[Col[β]]]]
    with InputWithEmptyT[F, Parallel] {

  def create[A: ClassTag](props: F[Col[A]])(implicit F: Monad[F]): DataPipelineT[F, A, Parallel] =
    LiftPipeline[F, Parallel].liftIterableF[A](props)

  def empty[A: ClassTag](
      implicit F: Monad[F]
  ): DataPipelineT[F, A, Parallel] = create[A](F.pure[Col[A]](cbf(Nil).result().asInstanceOf[Col[A]]))
}

class RandomInput private[trembita] () extends InputT[Id, Sequential, RandomInput.Props] {
  def create[A: ClassTag](props: RandomInput.Props[A])(
      implicit F: Monad[Id]
  ): DataPipelineT[Id, A, Sequential] =
    EvaluatedSource.make[Id, A, Sequential](
      Vector.tabulate(props.count)(_ => props.nOpt.fold(ifEmpty = Random.nextInt())(Random.nextInt)).map(props.gen),
      F
    )
}

class RandomInputF[F[_]] private[trembita] (implicit ctgF: ClassTag[F[_]]) extends InputT[F, Sequential, RandomInput.PropsT[F, ?]] {
  private implicit def ctgFA[A: ClassTag]: ClassTag[F[A]] = ClassTag[F[A]](ctgF.runtimeClass)

  def create[A: ClassTag](props: RandomInput.PropsT[F, A])(
      implicit F: Monad[F]
  ): DataPipelineT[F, A, Sequential] =
    EvaluatedSource
      .make[F, F[A], Sequential](
        F.pure(Vector.tabulate(props.count)(_ => props.nOpt.fold(ifEmpty = Random.nextInt())(Random.nextInt)).map(props.gen)),
        F
      )
      .mapMImpl[F[A], A](identity)
}

object RandomInput {
  class PropsT[F[_], @specialized(Specializable.BestOfBreed) A] private[trembita] (
      private[trembita] val nOpt: Option[Int],
      private[trembita] val count: Int,
      private[trembita] val gen: Int => F[A]
  )
  type Props[A] = PropsT[Id, A]

  @inline def props(count: Int): Props[Int]                                       = propsT[Id, Int](count)(identity)
  @inline def props(n: Int, count: Int): Props[Int]                               = propsT[Id, Int](n, count)(identity)
  @inline def props[A](n: Int, count: Int)(gen: Int => A): Props[A]               = propsT[Id, A](n, count)(gen)
  @inline def propsT[F[_], A](count: Int)(gen: Int => F[A]): PropsT[F, A]         = new PropsT[F, A](None, count, gen)
  @inline def propsT[F[_], A](n: Int, count: Int)(gen: Int => F[A]): PropsT[F, A] = new PropsT[F, A](Some(n), count, gen)
}
