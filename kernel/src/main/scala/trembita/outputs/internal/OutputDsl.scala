package trembita.outputs.internal

import cats.kernel.Monoid
import cats.{~>, Applicative, Id, Monad}
import trembita._
import trembita.operations._
import trembita.outputs.Keep

import scala.collection.generic.CanBuildFrom
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait OutputDsl[F[_], Er, A, E <: Environment] extends Any with Serializable {
  def `this`: BiDataPipelineT[F, Er, A, E]

  @inline def into(output: OutputWithPropsT[F, Er, E]): outputWithPropsDsl[F, Er, A, E, output.Props, output.Out] =
    new outputWithPropsDsl[F, Er, A, E, output.Props, output.Out](
      (`this`, output.widen)
    )

  @inline def into(
      output: OutputT[F, Er, A, E]
  ): outputWithoutPropsDsl[F, Er, A, E, output.Out] =
    new outputWithoutPropsDsl[F, Er, A, E, output.Out](
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
class outputWithPropsDsl[F[_], Er, A, E <: Environment, P0[_], Out0[_[_], _]](
    val `this`: (BiDataPipelineT[F, Er, A, E], OutputWithPropsT.Aux[F, Er, E, P0, Out0])
) extends AnyVal
    with Serializable {
  @inline def run(implicit props: P0[A], E: E, run: E#Run[F], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    `this`._2[Er, A](props)(`this`._1)

  @inline def run(props: P0[A])(implicit _run: E#Run[F], E: E, F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    run(props, E, _run, F, A)

  @inline def alsoInto(
      output: OutputWithPropsT[F, Er, E]
  ): keepDslWithProps[F, Er, A, E, P0, Out0, output.Props, output.Out] =
    new keepDslWithProps[F, Er, A, E, P0, Out0, output.Props, output.Out](
      (`this`._1, `this`._2, output.widen)
    )

  @inline def alsoInto(
      output: OutputT[F, Er, A, E]
  ): keepDslCombinedLeft[F, Er, A, E, P0, Out0, output.Out] = new keepDslCombinedLeft[F, Er, A, E, P0, Out0, output.Out](
    (`this`._1, `this`._2, output.widen)
  )
}

class keepDslWithProps[F[_], Er, A, E <: Environment, P0[_], Out0[_[_], _], P1[_], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, Er, A, E], OutputWithPropsT.Aux[F, Er, E, P0, Out0], OutputWithPropsT.Aux[F, Er, E, P1, Out1])
) extends AnyVal
    with Serializable {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, Er, A, E, λ[β => (P0[β], P1[A])], Out0] =
    new outputWithPropsDsl[F, Er, A, E, λ[β => (P0[β], P1[A])], Out0](
      (
        `this`._1,
        Keep
          .left[Out0, Out1]
          .newOutputWithProps[F, Er, E, P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, Er, E, λ[β => (P0[β], P1[A])], Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, Er, A, E, λ[β => (P0[β], P1[A])], Out1] =
    new outputWithPropsDsl[F, Er, A, E, λ[β => (P0[β], P1[A])], Out1](
      (
        `this`._1,
        Keep
          .right[Out0, Out1]
          .newOutputWithProps[F, Er, E, P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, Er, E, λ[β => (P0[β], P1[A])], Out1]]
      )
    )

  @inline def keepBoth: outputWithPropsDsl[F, Er, A, E, λ[β => (P0[β], P1[A])], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] =
    new outputWithPropsDsl[F, Er, A, E, λ[β => (P0[β], P1[A])], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
      (
        `this`._1,
        Keep
          .both[Out0, Out1]
          .newOutputWithProps[F, Er, E, P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, Er, E, λ[β => (P0[β], P1[A])], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]]]
      )
    )
}

class outputWithoutPropsDsl[F[_], Er, A, E <: Environment, Out0[_[_], _]](
    val `this`: (BiDataPipelineT[F, Er, A, E], OutputT.Aux[F, Er, A, E, Out0])
) extends AnyVal
    with Serializable {
  @inline def run(implicit E: E, run: E#Run[F], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    `this`._2(`this`._1)

  @inline def alsoInto(output: OutputT[F, Er, A, E]): keepDslWithoutProps[F, Er, A, E, Out0, output.Out] =
    new keepDslWithoutProps[F, Er, A, E, Out0, output.Out](
      (`this`._1, `this`._2, output.widen)
    )

  @inline def alsoInto(
      output: OutputWithPropsT[F, Er, E]
  ): keepDslCombinedRight[F, Er, A, E, output.Props, output.Out, Out0] =
    new keepDslCombinedRight[F, Er, A, E, output.Props, output.Out, Out0](
      (`this`._1, output.widen, `this`._2)
    )
}

class keepDslCombinedLeft[F[_], Er, A, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, Er, A, E], OutputWithPropsT.Aux[F, Er, E, P0, Out0], OutputT.Aux[F, Er, A, E, Out1])
) extends AnyVal
    with Serializable {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, Er, A, E, P0, Out0] =
    new outputWithPropsDsl[F, Er, A, E, P0, Out0](
      (
        `this`._1,
        CombinedOutput(`this`._2.widen, Keep.left[Out0, Out1], `this`._3.widen)
          .asInstanceOf[OutputWithPropsT.Aux[F, Er, E, P0, Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, Er, A, E, P0, Out1] =
    new outputWithPropsDsl[F, Er, A, E, P0, Out1](
      (
        `this`._1,
        CombinedOutput(`this`._2.widen, Keep.right[Out0, Out1], `this`._3.widen)
          .asInstanceOf[OutputWithPropsT.Aux[F, Er, E, P0, Out1]]
      )
    )

  @inline def keepBoth: outputWithPropsDsl[F, Er, A, E, P0, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] =
    new outputWithPropsDsl[F, Er, A, E, P0, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
      (
        `this`._1,
        CombinedOutput[F, Er, A, E, P0, Out0, Out1, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
          `this`._2.widen,
          Keep.both[Out0, Out1],
          `this`._3.widen
        )
      )
    )

  @inline def ignoreBoth(
      implicit ev0: Out0[F, _] <:< F[_],
      ev1: Out1[F, _] <:< F[_]
  ): outputWithPropsDsl[F, Er, A, E, P0, λ[(G[_], A) => G[Unit]]] = new outputWithPropsDsl[F, Er, A, E, P0, λ[(G[_], A) => G[Unit]]](
    (
      `this`._1, {
        val keepBothOutput =
          CombinedOutput[F, Er, A, E, P0, Out0, Out1, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](`this`._2, Keep.both[Out0, Out1], `this`._3)

        val keepBindOutput =
          new OutputWithPropsT[F, Er, E] {
            type Props[β]     = P0[β]
            type Out[G[_], β] = G[Unit]
            def apply[Err >: Er, Ax: ClassTag](props: Props[Ax])(
                pipeline: BiDataPipelineT[F, Err, Ax, E]
            )(implicit F: Monad[F], E: E, run: E#Run[F]): F[Unit] = {
              val (left, right) = keepBothOutput[Err, Ax](props)(pipeline)
              F.productR(
                ev0(left)
              )(
                F.void(
                  ev1(right)
                )
              )
            }
          }.asInstanceOf[OutputWithPropsT.Aux[F, Er, E, P0, λ[(G[_], A) => G[Unit]]]]

        keepBindOutput
      }
    )
  )
}

class keepDslCombinedRight[F[_], Er, A, E <: Environment, P0[_], Out0[_[_], _], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, Er, A, E], OutputWithPropsT.Aux[F, Er, E, P0, Out0], OutputT.Aux[F, Er, A, E, Out1])
) extends AnyVal
    with Serializable {
  @inline private def leftDsl =
    new keepDslCombinedLeft[F, Er, A, E, P0, Out0, Out1](
      `this`
    )

  @inline def keepLeft(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, Er, A, E, P0, Out1] = leftDsl.keepRight

  @inline def keepRight(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, Er, A, E, P0, Out0] = leftDsl.keepLeft

  @inline def keepBoth: outputWithPropsDsl[F, Er, A, E, P0, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] = leftDsl.keepBoth

  @inline def ignoreBoth(
      implicit ev0: Out0[F, _] <:< F[_],
      ev1: Out1[F, _] <:< F[_]
  ): outputWithPropsDsl[F, Er, A, E, P0, λ[(G[_], A) => G[Unit]]] = leftDsl.ignoreBoth
}

class keepDslWithoutProps[F[_], Er, A, E <: Environment, Out0[_[_], _], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, Er, A, E], OutputT.Aux[F, Er, A, E, Out0], OutputT.Aux[F, Er, A, E, Out1])
) extends AnyVal {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithoutPropsDsl[F, Er, A, E, Out0] =
    new outputWithoutPropsDsl[F, Er, A, E, Out0](
      (
        `this`._1,
        Keep
          .left[Out0, Out1]
          .newOutputWithoutProps[F, Er, A, E](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, Er, A, E, Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithoutPropsDsl[F, Er, A, E, Out1] =
    new outputWithoutPropsDsl[F, Er, A, E, Out1](
      (
        `this`._1,
        Keep
          .right[Out0, Out1]
          .newOutputWithoutProps[F, Er, A, E](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, Er, A, E, Out1]]
      )
    )

  @inline def keepBoth: outputWithoutPropsDsl[F, Er, A, E, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] =
    new outputWithoutPropsDsl[F, Er, A, E, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
      (
        `this`._1,
        Keep
          .both[Out0, Out1]
          .newOutputWithoutProps[F, Er, A, E](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, Er, A, E, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]]]
      )
    )

  @inline def ignoreBoth[Tx, Ux](
      implicit ev0: Out0[F, A] <:< F[Tx],
      ev1: Out1[F, A] <:< F[Ux]
  ): outputWithoutPropsDsl[F, Er, A, E, λ[(G[_], A) => G[Unit]]] = new outputWithoutPropsDsl[F, Er, A, E, λ[(G[_], A) => G[Unit]]](
    (
      `this`._1, {
        val keepIgnore = new OutputT[F, Er, A, E] {
          type Out[G[_], β] = G[Unit]

          def apply[Err >: Er](
              pipeline: BiDataPipelineT[F, Err, A, E]
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
        }.asInstanceOf[OutputT.Aux[F, Er, A, E, λ[(G[_], A) => G[Unit]]]]
        keepIgnore
      }
    )
  )
}

class collectionDsl[Col[x] <: Iterable[x]](val `dummy`: Boolean = true) extends AnyVal with Serializable

object collectionDsl {
  implicit def dslToSeqOutput[F[_], Er, Col[x] <: Iterable[x]](
      dsl: collectionDsl[Col]
  ): OutputWithPropsT.Aux[F, Er, Sequential, λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
    new SequentialCollectionOutput[Col, F, Er]()

  implicit def dslToParOutput[F[_], Er, Col[x] <: Iterable[x]](
      dsl: collectionDsl[Col]
  ): OutputWithPropsT.Aux[F, Er, Parallel, λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
    new ParallelCollectionOutput[Col, F, Er]()
}

class foreachDsl[A](val `f`: A => Unit) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment]() =
    new ForeachOutput[F, Er, A, E](`f`)
}

trait LowPriorityForeachConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment](dsl: foreachDsl[A]): OutputT.Aux[F, Er, A, E, λ[(F[_], β) => F[Unit]]] =
    dsl()
}

object foreachDsl extends LowPriorityForeachConversions {
  implicit def dslToOutputId[A, E <: Environment](dsl: foreachDsl[A]): OutputT.Aux[Id, Nothing, A, E, λ[(F[_], β) => F[Unit]]] =
    dsl()
}

class reduceDsl[A](val `f`: (A, A) => A) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment](ev: CanReduce[E#Repr])(arrow: ev.Result ~> F) =
    new ReduceOutput[F, Er, A, E, ev.Result](`f`)(ev)(arrow)
}

trait LowPriorityReduceConversions extends Serializable {
  implicit def dslToOutput[F[_], Er, A, E <: Environment, R0[_]](dsl: reduceDsl[A])(
      implicit canReduce: CanReduce.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[β]]] = dsl[F, Er, E](canReduce)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment](dsl: reduceDsl[A])(
      implicit canReduce: CanReduce.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[β]]] = dsl[F, Er, E](canReduce)(idTo[F])
}

object reduceDsl extends LowPriorityReduceConversions {
  implicit def dslToOutputId[A, E <: Environment](dsl: reduceDsl[A])(
      implicit canReduce: CanReduce.Aux[E#Repr, Id]
  ): OutputT.Aux[Id, Nothing, A, E, λ[(G[_], β) => G[β]]] = dsl[Id, Nothing, E](canReduce)(identityK[Id])
}

class reduceOptDsl[A](val `f`: (A, A) => A) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment](ev: CanReduce[E#Repr])(arrow: ev.Result ~> F) =
    new ReduceOptOutput[F, Er, A, E, ev.Result](`f`)(ev)(arrow)
}

trait LowPriorityReduceOptDsl extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment, R0[_]](dsl: reduceOptDsl[A])(
      implicit canReduce: CanReduce.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Option[β]]]] = dsl[F, Er, E](canReduce)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment](dsl: reduceOptDsl[A])(
      implicit canReduce: CanReduce.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Option[β]]]] = dsl[F, Er, E](canReduce)(idTo)
}

object reduceOptDsl extends LowPriorityReduceOptDsl {
  implicit def dslToOutputId[A, E <: Environment](dsl: reduceOptDsl[A])(
      implicit canReduce: CanReduce.Aux[E#Repr, Id]
  ): OutputT.Aux[Id, Nothing, A, E, λ[(G[_], β) => G[Option[β]]]] = dsl[Id, Nothing, E](canReduce)(identityK[Id])
}

class foldDsl[A](val `this`: (A, (A, A) => A)) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment](ev: CanFold[E#Repr])(arrow: ev.Result ~> F) =
    new FoldOutput[F, Er, A, E, ev.Result](`this`._1)(`this`._2)(ev)(arrow)
}

trait LowPriorityFoldConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment, R0[_]](dsl: foldDsl[A])(
      implicit canFold: CanFold.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[β]]] = dsl[F, Er, E](canFold)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment](dsl: foldDsl[A])(
      implicit canFold: CanFold.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[β]]] = dsl[F, Er, E](canFold)(idTo[F])
}

object foldDsl extends LowPriorityFoldConversions {
  implicit def dslToOutputId[A, E <: Environment](dsl: foldDsl[A])(
      implicit canFold: CanFold.Aux[E#Repr, Id]
  ): OutputT.Aux[Id, Nothing, A, E, λ[(G[_], β) => G[β]]] = dsl[Id, Nothing, E](canFold)(identityK[Id])
}

class foldLeftDsl[A, B](val `this`: (B, (B, A) => B)) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment](ev: CanFold[E#Repr])(arrow: ev.Result ~> F)(implicit B: ClassTag[B]) =
    new FoldLeftOutput[F, Er, A, B, E, ev.Result](`this`._1)(`this`._2)(ev)(arrow)
}

trait LowPriorityFoldLeftConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, B: ClassTag, E <: Environment, R0[_]](dsl: foldLeftDsl[A, B])(
      implicit canFold: CanFold.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[B]]] = dsl[F, Er, E](canFold)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, B: ClassTag, E <: Environment](dsl: foldLeftDsl[A, B])(
      implicit canFold: CanFold.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[B]]] = dsl[F, Er, E](canFold)(idTo[F])
}

object foldLeftDsl extends LowPriorityFoldLeftConversions {
  implicit def dslToOutputId[A, B: ClassTag, E <: Environment](dsl: foldLeftDsl[A, B])(
      implicit canFold: CanFold.Aux[E#Repr, Id]
  ): OutputT.Aux[Id, Nothing, A, E, λ[(G[_], β) => G[B]]] = dsl[Id, Nothing, E](canFold)(identityK[Id])
}

class foldFDsl[F[_], A, B](val `this`: (B, (B, A) => F[B])) extends AnyVal {
  def apply[Er, E <: Environment](implicit canFold: CanFoldF[E#Repr, F], B: ClassTag[B]) =
    new FoldFOutput[F, Er, A, B, E](`this`._1)(`this`._2)(canFold)
}

object foldFDsl {
  implicit def dslToOutputId[F[_], A, B: ClassTag, E <: Environment](dsl: foldFDsl[F, A, B])(
      implicit canFold: CanFoldF[E#Repr, F]
  ): OutputT.Aux[F, Nothing, A, E, λ[(G[_], β) => G[B]]] = dsl[Nothing, E]
}

class sizeDsl(val `dummy`: Boolean = true) extends AnyVal with Serializable {
  def apply[F[_], Er, A, E <: Environment](ev: HasSize[E#Repr])(arrow: ev.Result ~> F) =
    new SizeOutput[F, Er, A, E, ev.Result](ev)(arrow)

  def apply[F[_], Er, A, E <: Environment](ev: HasBigSize[E#Repr])(arrow: ev.Result ~> F) =
    new SizeOutput2[F, Er, A, E, ev.Result](ev)(arrow)
}

trait LowPrioritySizeConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment, R0[_]](dsl: sizeDsl)(
      implicit hasSize: HasSize.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Int]]] =
    dsl[F, Er, A, E](hasSize)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment, R0[_]](dsl: sizeDsl)(
      implicit hasSize: HasSize.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Int]]] =
    dsl[F, Er, A, E](hasSize)(idTo[F])

  implicit def dslBigSizeToOutputT[F[_], Er, A, E <: Environment, R0[_]](dsl: sizeDsl)(
      implicit hasSize: HasBigSize.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Long]]] =
    dsl[F, Er, A, E](hasSize)(arrow)

  implicit def dslToOutputBigSizeApplicative[F[_]: Applicative, Er, A, E <: Environment, R0[_]](dsl: sizeDsl)(
      implicit hasSize: HasBigSize.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Long]]] =
    dsl[F, Er, A, E](hasSize)(idTo[F])
}

object sizeDsl extends LowPrioritySizeConversions {
  implicit def dslToOutputId[A, E <: Environment, R0[_]](dsl: sizeDsl)(
      implicit hasSize: HasSize.Aux[E#Repr, Id]
  ): OutputT.Aux[Id, Nothing, A, E, λ[(G[_], β) => G[Int]]] =
    dsl[Id, Nothing, A, E](hasSize)(identityK[Id])

  implicit def dslToOutputIdBigSize[A, E <: Environment, R0[_]](dsl: sizeDsl)(
      implicit hasSize: HasBigSize.Aux[E#Repr, Id]
  ): OutputT.Aux[Id, Nothing, A, E, λ[(G[_], β) => G[Long]]] =
    dsl[Id, Nothing, A, E](hasSize)(identityK[Id])
}
