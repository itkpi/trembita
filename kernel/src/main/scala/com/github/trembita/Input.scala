package com.github.trembita

import cats.Id
import com.github.trembita.inputs._
import com.github.trembita.operations.LiftPipeline

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.reflect.ClassTag

object Input {
  @inline def reprF[F[+ _], E <: Environment]: InputT[F, E, λ[β => F[E#Repr[β]]]]                             = new ReprInputF[F, E]
  @inline def repeatF[F[+ _]]: InputT[F, Sequential, RepeatInput.PropsT[F, ?]]                                = new RepeatInputT[F]
  @inline def fileF[F[+ _]]: InputT[F, Sequential, FileInput.PropsT[F, ?]]                                    = new FileInputF[F]
  @inline def randomF[F[+ _]](implicit ctgF: ClassTag[F[_]]): InputT[F, Sequential, RandomInput.PropsT[F, ?]] = new RandomInputF[F]

  @inline def lift[E <: Environment](implicit liftPipeline: LiftPipeline[Id, E])       = liftF[Id, E]
  @inline def liftF[F[_], E <: Environment](implicit liftPipeline: LiftPipeline[F, E]) = new liftDsl[F, E](liftPipeline)

  class liftDsl[F[_], E <: Environment](val `this`: LiftPipeline[F, E]) extends AnyVal {
    def create[A: ClassTag](xs: Iterable[A]): DataPipelineT[F, A, E]     = `this`.liftIterable(xs)
    def createF[A: ClassTag](fa: F[Iterable[A]]): DataPipelineT[F, A, E] = `this`.liftIterableF(fa)
  }

  @inline def sequentialF[F[+ _], Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[F, Sequential, λ[β => F[Col[β]]]] with InputWithEmptyT[F, Sequential] =
    new IterableInputF[F, Col]

  @inline def parallelF[F[+ _], Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[F, Parallel, λ[β => F[Col[β]]]] with InputWithEmptyT[F, Parallel] =
    new ParIterableInputF[F, Col]

  @inline def repr[E <: Environment]: InputT[Id, E, E#Repr] = new ReprInput[E]

  @inline def random[A]: InputT[Id, Sequential, RandomInput.Props] = new RandomInput
  val repeat: InputT[Id, Sequential, RepeatInput.Props]            = new RepeatInput
  val file: InputT[Id, Sequential, FileInput.Props]                = new FileInput

  def sequential[Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[Id, Sequential, Col] with InputWithEmptyT[Id, Sequential] =
    new IterableInput[Col]

  def parallel[Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[Id, Parallel, Col] with InputWithEmptyT[Id, Parallel] = new ParIterableInput[Col]
}
