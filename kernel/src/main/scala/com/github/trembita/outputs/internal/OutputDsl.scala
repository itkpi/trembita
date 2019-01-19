package com.github.trembita.outputs.internal

import cats.Monad
import com.github.trembita._
import com.github.trembita.outputs.Keep
import scala.collection.generic.CanBuildFrom
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait OutputDsl[F[_], A, E <: Environment] extends Any {
  def `this`: DataPipelineT[F, A, E]

  @inline def into(output: OutputWithPropsT[F, E]): outputWithPropsDsl[F, A, E, output.Props, output.Out] =
    new outputWithPropsDsl[F, A, E, output.Props, output.Out](
      (`this`, output.widen)
    )

  @inline def into(
      output: OutputT[F, A, E]
  ): outputWithoutPropsDsl[F, A, E, output.Out] =
    new outputWithoutPropsDsl[F, A, E, output.Out](
      (`this`, output.widen)
    )
}

/*
 * ===================================================================
 *                            DSL INTERNALS
 * -------------------------------------------------------------------
 *                         ⚠️⚠️⚠️ WARNING ⚠️⚠️⚠️
 *🚨
 *                  ⚡⚡⚡⚡  HIGH LEVEL WTF CODE ⚡⚡⚡⚡⚡
 *
 *                Read only home with a cup of coffee ☕
 *           Your customer needs you to feel yourself calm 😇😇😇
 *
 * ===================================================================
 * */
class outputWithPropsDsl[F[_], A, E <: Environment, P0[_], Out0[_[_], _]](
    val `this`: (DataPipelineT[F, A, E], OutputWithPropsT.Aux[F, E, P0, Out0])
) extends AnyVal {
  @inline def run(implicit props: P0[A], E: E, run: E#Run[F], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    `this`._2[A](props)(`this`._1)

  @inline def run(props: P0[A])(implicit _run: E#Run[F], E: E, F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    run(props, E, _run, F, A)

  @inline def alsoInto(
      output: OutputWithPropsT[F, E]
  ): keepDslWithProps[F, A, E, P0, Out0, output.Props, output.Out] = new keepDslWithProps[F, A, E, P0, Out0, output.Props, output.Out](
    (`this`._1, `this`._2, output.widen)
  )

  @inline def alsoInto(
      output: OutputT[F, A, E]
  ): keepDslCombinedLeft[F, A, E, P0, Out0, output.Out] = new keepDslCombinedLeft[F, A, E, P0, Out0, output.Out](
    (`this`._1, `this`._2, output.widen)
  )
}

class keepDslWithProps[F[_], A, E <: Environment, P0[_], Out0[_[_], _], P1[_], Out1[_[_], _]](
    val `this`: (DataPipelineT[F, A, E], OutputWithPropsT.Aux[F, E, P0, Out0], OutputWithPropsT.Aux[F, E, P1, Out1])
) extends AnyVal {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, A, E, λ[a => (P0[a], P1[A])], Out0] =
    new outputWithPropsDsl[F, A, E, λ[a => (P0[a], P1[A])], Out0](
      (
        `this`._1,
        Keep
          .left[Out0, Out1]
          .newOutputWithProps[F, E, P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, E, λ[a => (P0[a], P1[A])], Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, A, E, λ[a => (P0[a], P1[A])], Out1] =
    new outputWithPropsDsl[F, A, E, λ[a => (P0[a], P1[A])], Out1](
      (
        `this`._1,
        Keep
          .right[Out0, Out1]
          .newOutputWithProps[F, E, P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, E, λ[a => (P0[a], P1[A])], Out1]]
      )
    )

  @inline def keepBoth: outputWithPropsDsl[F, A, E, λ[a => (P0[a], P1[A])], λ[(G[_], a) => (Out0[G, a], Out1[G, a])]] =
    new outputWithPropsDsl[F, A, E, λ[a => (P0[a], P1[A])], λ[(G[_], a) => (Out0[G, a], Out1[G, a])]](
      (
        `this`._1,
        Keep
          .both[Out0, Out1]
          .newOutputWithProps[F, E, P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, E, λ[a => (P0[a], P1[A])], λ[(G[_], a) => (Out0[G, a], Out1[G, a])]]]
      )
    )
}

class outputWithoutPropsDsl[F[_], A, E <: Environment, Out0[_[_], _]](
    val `this`: (DataPipelineT[F, A, E], OutputT.Aux[F, A, E, Out0])
) extends AnyVal {
  @inline def run(implicit E: E, run: E#Run[F], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    `this`._2(`this`._1)

  @inline def alsoInto(output: OutputT[F, A, E]): keepDslWithoutProps[F, A, E, Out0, output.Out] =
    new keepDslWithoutProps[F, A, E, Out0, output.Out](
      (`this`._1, `this`._2, output.widen)
    )

  @inline def alsoInto(
      output: OutputWithPropsT[F, E]
  ): keepDslCombinedRight[F, A, E, output.Props, output.Out, Out0] = new keepDslCombinedRight[F, A, E, output.Props, output.Out, Out0](
    (`this`._1, output.widen, `this`._2)
  )
}

class keepDslCombinedLeft[F[_], A, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _]](
    val `this`: (DataPipelineT[F, A, E], OutputWithPropsT.Aux[F, E, P0, Out0], OutputT.Aux[F, A, E, Out1])
) extends AnyVal {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, A, E, P0, Out0] =
    new outputWithPropsDsl[F, A, E, P0, Out0](
      (
        `this`._1,
        CombinedOutput(`this`._2.widen, Keep.left[Out0, Out1], `this`._3.widen)
          .asInstanceOf[OutputWithPropsT.Aux[F, E, P0, Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, A, E, P0, Out1] =
    new outputWithPropsDsl[F, A, E, P0, Out1](
      (
        `this`._1,
        CombinedOutput(`this`._2.widen, Keep.right[Out0, Out1], `this`._3.widen)
          .asInstanceOf[OutputWithPropsT.Aux[F, E, P0, Out1]]
      )
    )

  @inline def keepBoth: outputWithPropsDsl[F, A, E, P0, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]] =
    new outputWithPropsDsl[F, A, E, P0, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]](
      (
        `this`._1,
        CombinedOutput[F, A, E, P0, Out0, Out1, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]](
          `this`._2.widen,
          Keep.both[Out0, Out1],
          `this`._3.widen
        )
      )
    )

  @inline def ignoreBoth(
      implicit ev0: Out0[F, _] <:< F[_],
      ev1: Out1[F, _] <:< F[_]
  ): outputWithPropsDsl[F, A, E, P0, λ[(G[_], A) => G[Unit]]] = new outputWithPropsDsl[F, A, E, P0, λ[(G[_], A) => G[Unit]]](
    (
      `this`._1, {
        val keepBothOutput =
          CombinedOutput[F, A, E, P0, Out0, Out1, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]](`this`._2, Keep.both[Out0, Out1], `this`._3)

        val keepBindOutput =
          new OutputWithPropsT[F, E] {
            type Props[a]     = P0[a]
            type Out[G[_], a] = G[Unit]
            def apply[Ax: ClassTag](props: Props[Ax])(
                pipeline: DataPipelineT[F, Ax, E]
            )(implicit F: Monad[F], E: E, run: E#Run[F]): F[Unit] = {
              val (left, right) = keepBothOutput[Ax](props)(pipeline)
              F.productR(
                ev0(left)
              )(
                F.void(
                  ev1(right)
                )
              )
            }
          }.asInstanceOf[OutputWithPropsT.Aux[F, E, P0, λ[(G[_], A) => G[Unit]]]]

        keepBindOutput
      }
    )
  )
}

class keepDslCombinedRight[F[_], A, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _]](
    val `this`: (DataPipelineT[F, A, E], OutputWithPropsT.Aux[F, E, P0, Out0], OutputT.Aux[F, A, E, Out1])
) extends AnyVal {
  @inline private def leftDsl =
    new keepDslCombinedLeft[F, A, E, P0, Out0, Out1](
      `this`
    )

  @inline def keepLeft(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, A, E, P0, Out1] = leftDsl.keepRight

  @inline def keepRight(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, A, E, P0, Out0] = leftDsl.keepLeft

  @inline def keepBoth: outputWithPropsDsl[F, A, E, P0, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]] = leftDsl.keepBoth

  @inline def ignoreBoth(
      implicit ev0: Out0[F, _] <:< F[_],
      ev1: Out1[F, _] <:< F[_]
  ): outputWithPropsDsl[F, A, E, P0, λ[(G[_], A) => G[Unit]]] = leftDsl.ignoreBoth
}

class keepDslWithoutProps[F[_], A, E <: Environment, Out0[_[_], _], Out1[_[_], _]](
    val `this`: (DataPipelineT[F, A, E], OutputT.Aux[F, A, E, Out0], OutputT.Aux[F, A, E, Out1])
) extends AnyVal {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithoutPropsDsl[F, A, E, Out0] =
    new outputWithoutPropsDsl[F, A, E, Out0](
      (
        `this`._1,
        Keep
          .left[Out0, Out1]
          .newOutputWithoutProps[F, A, E](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, A, E, Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithoutPropsDsl[F, A, E, Out1] =
    new outputWithoutPropsDsl[F, A, E, Out1](
      (
        `this`._1,
        Keep
          .right[Out0, Out1]
          .newOutputWithoutProps[F, A, E](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, A, E, Out1]]
      )
    )

  @inline def keepBoth: outputWithoutPropsDsl[F, A, E, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]] =
    new outputWithoutPropsDsl[F, A, E, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]](
      (
        `this`._1,
        Keep
          .both[Out0, Out1]
          .newOutputWithoutProps[F, A, E](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, A, E, λ[(G[_], a) => (Out0[G, a], Out1[G, a])]]]
      )
    )

  @inline def ignoreBoth[Tx, Ux](
      implicit ev0: Out0[F, A] <:< F[Tx],
      ev1: Out1[F, A] <:< F[Ux]
  ): outputWithoutPropsDsl[F, A, E, λ[(G[_], A) => G[Unit]]] = new outputWithoutPropsDsl[F, A, E, λ[(G[_], A) => G[Unit]]](
    (
      `this`._1, {
        val keepIgnore = new OutputT[F, A, E] {
          type Out[G[_], a] = G[Unit]

          def apply(
              pipeline: DataPipelineT[F, A, E]
          )(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): F[Unit] = {
            val ppln  = pipeline.memoize()
            val left  = `this`._2(ppln)
            val right = `this`._3(ppln)
            F.productR(
              ev0(left)
            )(
              F.void(
                ev1(right)
              )
            )
          }
        }.asInstanceOf[OutputT.Aux[F, A, E, λ[(G[_], A) => G[Unit]]]]
        keepIgnore
      }
    )
  )
}

class collectionDsl[Col[x] <: Iterable[x]](val `dummy`: Boolean = true) extends AnyVal

object collectionDsl {
  implicit def dslToSeqOutput[F[_], Col[x] <: Iterable[x]](
      dsl: collectionDsl[Col]
  ): OutputWithPropsT.Aux[F, Sequential, λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
    new SequentialCollectionOutput[Col, F]()

  implicit def dslToParOutput[F[_], Col[x] <: Iterable[x]](
      dsl: collectionDsl[Col]
  ): OutputWithPropsT.Aux[F, Parallel, λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
    new ParallelCollectionOutput[Col, F]()
}

class foreachDsl[A](val `f`: A => Unit) extends AnyVal {
  @inline def apply[F[_], E <: Environment]() =
    new ForeachOutput[F, A, E](`f`)
}

object foreachDsl {
  implicit def dslToOutput[F[_], A, E <: Environment](dsl: foreachDsl[A]): OutputT.Aux[F, A, E, λ[(F[_], a) => F[Unit]]] =
    dsl()
}

trait standardOutputs {
  object Output {
    @inline def collection[Col[x] <: Iterable[x]] = new collectionDsl[Col]
    @inline def foreach[A](f: A => Unit)          = new foreachDsl[A](f)
  }
}
