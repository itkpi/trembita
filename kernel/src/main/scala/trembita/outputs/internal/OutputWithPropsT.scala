package trembita.outputs.internal

import cats.Monad
import trembita.outputs.Keep
import trembita.{BiDataPipelineT, Environment}

import scala.language.higherKinds
import scala.reflect.ClassTag

trait BaseOutputT[F[_], +Er, +A, E <: Environment] extends Serializable {
  type Out[G[_], x]
}

object BaseOutputT {
  type Aux[F[_], +Er, A, E <: Environment, Out0[_[_], _]] = BaseOutputT[F, Er, A, E] { type Out[G[_], x] = Out0[G, x] }
}

trait OutputWithPropsT[F[_], +Er, E <: Environment] extends BaseOutputT[F, Er, Nothing, E] {
  type Props[x]

  def apply[Err >: Er, A: ClassTag](props: Props[A])(
      pipeline: BiDataPipelineT[F, Err, A, E]
  )(implicit F: Monad[F], E: E, run: E#Run[F]): Out[F, A]

  def widen: OutputWithPropsT.Aux[F, Er, E, Props, Out] = this
}

object OutputWithPropsT {
  type AuxProps[F[_], +Er, E <: Environment, P0[_]] = OutputWithPropsT[F, Er, E] { type Props[A] = P0[A] }

  type AuxOut[F[_], +Er, E <: Environment, Out0[_[_], _]] = OutputWithPropsT[F, Er, E] { type Out[G[_], A] = Out0[G, A] }

  type Aux[F[_], +Er, E <: Environment, P0[_], Out0[_[_], _]] = OutputWithPropsT[F, Er, E] {
    type Props[A]     = P0[A]
    type Out[G[_], A] = Out0[G, A]
  }

  def apply[F[_], Er, E <: Environment](implicit ev: OutputWithPropsT[F, Er, E]): Aux[F, Er, E, ev.Props, ev.Out] = ev
}

trait OutputT[F[_], +Er, A, E <: Environment] extends BaseOutputT[F, Er, A, E] {
  def apply[Err >: Er](pipeline: BiDataPipelineT[F, Err, A, E])(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): Out[F, A]

  def widen: OutputT.Aux[F, Er, A, E, Out] = this
}

object OutputT {
  type Aux[F[_], +Er, A, E <: Environment, Out0[_[_], _]] = OutputT[F, Er, A, E] { type Out[G[_], x] = Out0[G, x] }

  def apply[F[_], Er, A, E <: Environment](implicit ev: OutputT[F, Er, A, E]): OutputT[F, Er, A, E] = ev
}

class CombinedOutput[F[_], +Er, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _], OutR[_[_], _]](
    outputWithProps: OutputWithPropsT.Aux[F, Er, E, P0, Out0],
    keep: Keep.Aux[Out0, Out1, OutR],
    outputWithoutProps: OutputT.Aux[F, Er, Any, E, Out1]
) extends OutputWithPropsT[F, Er, E] {

  final type Props[A]     = P0[A]
  final type Out[G[_], A] = OutR[G, A]

  @inline def apply[Err >: Er, A: ClassTag](
      props: Props[A]
  )(pipeline: BiDataPipelineT[F, Err, A, E])(implicit F: Monad[F], E: E, run: E#Run[F]): Out[F, A] = {
    val ppln  = pipeline.memoize()
    val left  = outputWithProps.apply[Err, A](props)(ppln)
    val right = outputWithoutProps.apply[Err](ppln.asInstanceOf[BiDataPipelineT[F, Er, Any, E]])
    keep[F, A](left, right.asInstanceOf[Out1[F, A]])
  }
}

object CombinedOutput {
  @inline def apply[F[_], Er, A, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _], OutR[_[_], _]](
      outputWithProps: OutputWithPropsT.Aux[F, Er, E, P0, Out0],
      keep: Keep.Aux[Out0, Out1, OutR],
      outputWithoutProps: OutputT.Aux[F, Er, A, E, Out1]
  ): CombinedOutput[F, Er, E, P0, Out0, Out1, OutR] = new CombinedOutput[F, Er, E, P0, Out0, Out1, OutR](
    outputWithProps,
    keep,
    outputWithoutProps.asInstanceOf[OutputT.Aux[F, Er, Any, E, Out1]]
  )
}
