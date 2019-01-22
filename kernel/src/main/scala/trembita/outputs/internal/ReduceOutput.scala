package trembita.outputs.internal

import cats.{~>, Monad}
import trembita._
import trembita.operations.{CanFold, HasSize}
import scala.reflect.ClassTag
import scala.language.higherKinds

class ReduceOutput[F[_], @specialized(Specializable.BestOfBreed) A, E <: Environment, R0[_]](f: (A, A) => A)(
    canFold: CanFold.Aux[E#Repr, R0]
)(arrow: R0 ~> F)
    extends OutputT[F, A, E] {
  final type Out[G[_], β] = G[β]

  def apply(
      pipeline: DataPipelineT[F, A, E]
  )(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[A] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(canFold.reduce(repr)(f)))
}

class ReduceOptOutput[F[_], @specialized(Specializable.BestOfBreed) A, E <: Environment, R0[_]](f: (A, A) => A)(
    canFold: CanFold.Aux[E#Repr, R0]
)(arrow: R0 ~> F)
    extends OutputT[F, A, E] {
  type Out[G[_], β] = G[Option[β]]

  def apply(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[Option[A]] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(canFold.reduceOpt(repr)(f)))
}

class FoldOutput[F[_], @specialized(Specializable.BestOfBreed) A, E <: Environment, R0[_]](zero: A)(f: (A, A) => A)(
    canFold: CanFold.Aux[E#Repr, R0]
)(arrow: R0 ~> F)
    extends OutputT[F, A, E] {
  type Out[G[_], β] = G[β]

  def apply(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[A] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(canFold.fold(repr)(zero)(f)))
}

class FoldLeftOutput[
    F[_],
    @specialized(Specializable.BestOfBreed) A,
    @specialized(Specializable.BestOfBreed) B: ClassTag,
    E <: Environment,
    R0[_]
](zero: B)(f: (B, A) => B)(canFold: CanFold.Aux[E#Repr, R0])(
    arrow: R0 ~> F
) extends OutputT[F, A, E] {
  type Out[G[_], β] = G[B]

  def apply(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[B] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(canFold.foldLeft(repr)(zero)(f)))
}

class SizeOutput[F[_], @specialized(Specializable.BestOfBreed) A, E <: Environment, R0[_]](hasSize: HasSize.Aux[E#Repr, R0])(arrow: R0 ~> F)
    extends OutputT[F, A, E] {
  type Out[G[_], β] = G[Int]

  def apply(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[Int] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(hasSize.size(repr)))
}
