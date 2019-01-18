package com.github.trembita.outputs.internal

import cats.Monad
import com.github.trembita.outputs.Keep
import com.github.trembita.{DataPipelineT, Environment}

import scala.language.higherKinds
import scala.reflect.ClassTag

trait BaseOutputT[F[_], +A, E <: Environment] {
  type Out[G[_], x]
}

object BaseOutputT {
  type Aux[F[_], A, E <: Environment, Out0[_[_], _]] = BaseOutputT[F, A, E] { type Out[G[_], x] = Out0[G, x] }
}

trait OutputWithPropsT[F[_], E <: Environment] extends BaseOutputT[F, Nothing, E] {
  type Props[x]

  def apply[A: ClassTag](props: Props[A])(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F]): Out[F, A]

  def widen: OutputWithPropsT.Aux[F, E, Props, Out] = this
}

object OutputWithPropsT {
  type AuxProps[F[_], E <: Environment, P0[_]] = OutputWithPropsT[F, E] { type Props[A] = P0[A] }

  type AuxOut[F[_], E <: Environment, Out0[_[_], _]] = OutputWithPropsT[F, E] { type Out[G[_], A] = Out0[G, A] }

  type Aux[F[_], E <: Environment, P0[_], Out0[_[_], _]] = OutputWithPropsT[F, E] {
    type Props[A]     = P0[A]
    type Out[G[_], A] = Out0[G, A]
  }

  def apply[F[_], E <: Environment](implicit ev: OutputWithPropsT[F, E]): Aux[F, E, ev.Props, ev.Out] = ev
}

trait OutputT[F[_], A, E <: Environment] extends BaseOutputT[F, A, E] {
  def apply(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): Out[F, A]

  def widen: OutputT.Aux[F, A, E, Out] = this
}

object OutputT {
  type Aux[F[_], A, E <: Environment, Out0[_[_], _]] = OutputT[F, A, E] { type Out[G[_], x] = Out0[G, x] }

  def apply[F[_], A, E <: Environment](implicit ev: OutputT[F, A, E]): OutputT[F, A, E] = ev
}

class CombinedOutput[F[_], E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _], OutR[_[_], _]](
    outputWithProps: OutputWithPropsT.Aux[F, E, P0, Out0],
    keep: Keep.Aux[Out0, Out1, OutR],
    outputWithoutProps: OutputT.Aux[F, Any, E, Out1]
) extends OutputWithPropsT[F, E] {

  final type Props[A]     = P0[A]
  final type Out[G[_], A] = OutR[G, A]

  @inline def apply[A: ClassTag](
      props: Props[A]
  )(pipeline: DataPipelineT[F, A, E])(implicit F: Monad[F], E: E, run: E#Run[F]): Out[F, A] = {
    val ppln  = pipeline.memoize()
    val left  = outputWithProps.apply(props)(ppln)
    val right = outputWithoutProps.apply(ppln.asInstanceOf[DataPipelineT[F, Any, E]])
    keep[F, A](left, right.asInstanceOf[Out1[F, A]])
  }
}

object CombinedOutput {
  @inline def apply[F[_], A, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _], OutR[_[_], _]](
      outputWithProps: OutputWithPropsT.Aux[F, E, P0, Out0],
      keep: Keep.Aux[Out0, Out1, OutR],
      outputWithoutProps: OutputT.Aux[F, A, E, Out1]
  ): CombinedOutput[F, E, P0, Out0, Out1, OutR] = new CombinedOutput[F, E, P0, Out0, Out1, OutR](
    outputWithProps,
    keep,
    outputWithoutProps.asInstanceOf[OutputT.Aux[F, Any, E, Out1]]
  )
}
