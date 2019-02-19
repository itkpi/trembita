package trembita

import java.io.IOException
import cats.{Id, Monad}
import trembita.inputs._
import trembita.operations.LiftPipeline
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.reflect.ClassTag

object Input {
  @inline def reprF[F[+ _], Er, E <: Environment]: InputT[F, Er, E, λ[β => F[E#Repr[β]]]] = new ReprInputF[F, Er, E]
  @inline def repeatF[F[+ _], Er]: InputT[F, Er, Sequential, RepeatInput.PropsT[F, ?]]    = new RepeatInputT[F, Er]
  @inline def fileF[F[+ _]]: InputT[F, IOException, Sequential, FileInput.PropsT[F, ?]]   = new FileInputF[F]
  @inline def randomF[F[+ _], Er](implicit ctgF: ClassTag[F[_]]): InputT[F, Er, Sequential, RandomInput.PropsT[F, ?]] =
    new RandomInputF[F, Er]

  @inline def lift[E <: Environment](implicit liftPipeline: LiftPipeline[Id, Nothing, E])      = liftF[Id, Nothing, E]
  @inline def liftF[F[_], Er, E <: Environment](implicit liftPipeline: LiftPipeline[F, Er, E]) = new liftDsl[F, Er, E](liftPipeline)

  class liftDsl[F[_], Er, E <: Environment](val `this`: LiftPipeline[F, Er, E]) extends AnyVal {
    def create[A: ClassTag](xs: Iterable[A]): BiDataPipelineT[F, Er, A, E]                           = `this`.liftIterable(xs)
    def createF[A: ClassTag](fa: F[Iterable[A]])(implicit F: Monad[F]): BiDataPipelineT[F, Er, A, E] = `this`.liftIterableF(fa)
  }

  @inline def sequentialF[F[+ _], Er, Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[F, Er, Sequential, λ[β => F[Col[β]]]] with InputWithEmptyT[F, Er, Sequential] =
    new IterableInputF[F, Er, Col]

  @inline def parallelF[F[+ _], Er, Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[F, Er, Parallel, λ[β => F[Col[β]]]] with InputWithEmptyT[F, Er, Parallel] =
    new ParIterableInputF[F, Er, Col]

  @inline def repr[E <: Environment]: InputT[Id, Nothing, E, E#Repr] = new ReprInput[E]

  @inline def random[A]: InputT[Id, Nothing, Sequential, RandomInput.Props] = new RandomInput
  val repeat: InputT[Id, Nothing, Sequential, RepeatInput.Props]            = new RepeatInput
  val file: InputT[Id, Nothing, Sequential, FileInput.Props]                = new FileInput

  def sequential[Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[Id, Nothing, Sequential, Col] with InputWithEmptyT[Id, Nothing, Sequential] =
    new IterableInput[Col]

  def parallel[Col[+x] <: Iterable[x]](
      implicit cbf: CanBuildFrom[Col[_], _, Col[_]]
  ): InputT[Id, Nothing, Parallel, Col] with InputWithEmptyT[Id, Nothing, Parallel] = new ParIterableInput[Col]
}
