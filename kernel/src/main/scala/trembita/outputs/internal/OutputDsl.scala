package trembita.outputs.internal

import cats.kernel.Monoid
import cats.{~>, Applicative, Id, Monad, MonadError}
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
  @inline def run(implicit props: P0[A], E: E, run: E#Run[F], F: MonadError[F, Er], A: ClassTag[A]): Out0[F, A] =
    `this`._2[Er, A](props)(`this`._1)

  @inline def run(props: P0[A])(implicit _run: E#Run[F], E: E, F: MonadError[F, Er], A: ClassTag[A]): Out0[F, A] =
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
  @inline def run(implicit E: E, run: E#Run[F], F: MonadError[F, Er], A: ClassTag[A]): Out0[F, A] =
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
            )(implicit F: MonadError[F, Er], E: E, run: E#Run[F]): F[Unit] = {
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

          def apply[Err >: Er, AA >: A](
              pipeline: BiDataPipelineT[F, Err, AA, E]
          )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Unit] = {
            val ppln  = pipeline.memoize()(F.asInstanceOf[MonadError[F, Err]], E, run, implicitly)
            val left  = `this`._2(ppln)
            val right = `this`._3(ppln)
            F.productR(
              ev0(left.asInstanceOf[Out0[F, A]])
            )(
              F.void(
                ev1(right.asInstanceOf[Out1[F, A]])
              )
            )
          }
        }.asInstanceOf[OutputT.Aux[F, Er, A, E, λ[(G[_], A) => G[Unit]]]]
        keepIgnore
      }
    )
  )
}

class collectionDsl[Col[x] <: Iterable[x]](val `not dummy`: Boolean = false) extends AnyVal with Serializable {
  def ignoreErrors: collectionIgnoreErrorsDsl[Col]     = new collectionIgnoreErrorsDsl[Col]()
  def withErrors[Er]: collectionWithErrorsDsl[Er, Col] = new collectionWithErrorsDsl[Er, Col]()
}

class collectionWithErrorsDsl[Er, Col[x] <: Iterable[x]](val `dummy`: Boolean = true) extends AnyVal with Serializable

object collectionWithErrorsDsl {
  implicit def collectionDslToSeqOutput[F[_], Er, Col[x] <: Iterable[x]](
      dsl: collectionWithErrorsDsl[Er, Col]
  ): OutputWithPropsT.Aux[F, Er, Sequential, λ[A => CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]]], λ[
    (G[_], A) => G[Col[Either[Er, A]]]
  ]] =
    new SequentialCollectionOutput[Col, F, Er]()

  implicit def collectionDslToParOutput[F[_], Er, Col[x] <: Iterable[x]](
      dsl: collectionWithErrorsDsl[Er, Col]
  ): OutputWithPropsT.Aux[F, Er, Parallel, λ[A => CanBuildFrom[Nothing, Either[Er, A], Col[Either[Er, A]]]], λ[
    (G[_], A) => G[Col[Either[Er, A]]]
  ]] =
    new ParallelCollectionOutput[Col, F, Er]()
}

class collectionIgnoreErrorsDsl[Col[x] <: Iterable[x]](val `dummy`: Boolean = true) extends AnyVal with Serializable

object collectionIgnoreErrorsDsl {
  implicit def collectionIgnoreErrorsDslToSeqOutput[F[_], Er, Col[x] <: Iterable[x]](
      dsl: collectionIgnoreErrorsDsl[Col]
  ): OutputWithPropsT.Aux[F, Er, Sequential, λ[A => CanBuildFrom[Nothing, A, Col[A]]], λ[
    (G[_], A) => G[Col[A]]
  ]] =
    new SequentialIgnoreErrorsCollectionOutput[Col, F, Er]()

  implicit def collectionIgnoreErrorsDslToParOutput[F[_], Er, Col[x] <: Iterable[x]](
      dsl: collectionIgnoreErrorsDsl[Col]
  ): OutputWithPropsT.Aux[F, Er, Parallel, λ[A => CanBuildFrom[Nothing, A, Col[A]]], λ[
    (G[_], A) => G[Col[A]]
  ]] =
    new ParallelIgnoreErrorsCollectionOutput[Col, F, Er]()
}

class foreachDsl[A](val `f`: A => Unit) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment]() =
    new ForeachOutput[F, Er, A, E](`f`)
}

trait LowPriorityForeachConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment](dsl: foreachDsl[A]): OutputT.Aux[F, Er, A, E, λ[(F[_], β) => F[Unit]]] =
    dsl()
}

object foreachDsl extends LowPriorityForeachConversions

class onCompleteDsl[Er, A](val `f`: Either[Er, A] => Unit) extends AnyVal with Serializable {
  @inline def apply[F[_], E <: Environment]() =
    new OnCompleteOutput[F, Er, A, E](`f`)
}

trait LowPriorityOnCompleteConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment](dsl: onCompleteDsl[Er, A]): OutputT.Aux[F, Er, A, E, λ[(F[_], β) => F[Unit]]] =
    dsl()
}

object onCompleteDsl extends LowPriorityOnCompleteConversions

class reduceDsl[Er, A](val `f`: (A, A) => A) extends AnyVal with Serializable {
  @inline def apply[F[_], E <: Environment](ev: CanReduce[E#Repr])(arrow: ev.Result ~> F) =
    new ReduceOutput[F, Er, A, E, ev.Result](`f`)(ev)(arrow)
}

trait LowPriorityReduceConversions extends Serializable {
  implicit def dslToOutput[F[_], Er, A, E <: Environment, R0[_]](dsl: reduceDsl[Er, A])(
      implicit canReduce: CanReduce.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, β]]]] = dsl[F, E](canReduce)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment](dsl: reduceDsl[Er, A])(
      implicit canReduce: CanReduce.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, β]]]] = dsl[F, E](canReduce)(idTo[F])
}

object reduceDsl extends LowPriorityReduceConversions

class reduceOptDsl[Er, A](val `f`: (A, A) => A) extends AnyVal with Serializable {
  @inline def apply[F[_], E <: Environment](ev: CanReduce[E#Repr])(arrow: ev.Result ~> F) =
    new ReduceOptOutput[F, Er, A, E, ev.Result](`f`)(ev)(arrow)
}

trait LowPriorityReduceOptDsl extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, E <: Environment, R0[_]](dsl: reduceOptDsl[Er, A])(
      implicit canReduce: CanReduce.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Option[Either[Er, β]]]]] = dsl[F, E](canReduce)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment](dsl: reduceOptDsl[Er, A])(
      implicit canReduce: CanReduce.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Option[Either[Er, β]]]]] = dsl[F, E](canReduce)(idTo)
}

object reduceOptDsl extends LowPriorityReduceOptDsl

class foldDsl(val `dummy`: Boolean = true) extends AnyVal {
  def withErrors[Er, A](zero: A)(f: (A, A) => A): foldDslWithErrors[Er, A] = new foldDslWithErrors[Er, A](zero -> f)
}

class foldDslWithErrors[Er, A](val `this`: (A, (A, A) => A)) extends AnyVal with Serializable {
  @inline def apply[F[_], E <: Environment](ev: CanFold[E#Repr])(arrow: ev.Result ~> F) =
    new FoldOutput[F, Er, A, E, ev.Result](`this`._1)(`this`._2)(ev)(arrow)
}

trait LowPriorityFoldConversions extends Serializable {
  implicit def foldDslToOutputT[F[_], Er, A, E <: Environment, R0[_]](dsl: foldDslWithErrors[Er, A])(
      implicit canFold: CanFold.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, β]]]] = dsl[F, E](canFold)(arrow)

  implicit def foldDslToOutputApplicative[F[_]: Applicative, Er, A, E <: Environment](dsl: foldDslWithErrors[Er, A])(
      implicit canFold: CanFold.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, β]]]] = dsl[F, E](canFold)(idTo[F])
}

object foldDslWithErrors extends LowPriorityFoldConversions

class foldLeftDsl[A, B](val `this`: (B, (B, A) => B)) extends AnyVal with Serializable {
  @inline def apply[F[_], Er, E <: Environment](ev: CanFold[E#Repr])(arrow: ev.Result ~> F)(implicit B: ClassTag[B]) =
    new FoldLeftOutput[F, Er, A, B, E, ev.Result](`this`._1)(`this`._2)(ev)(arrow)
}

trait LowPriorityFoldLeftConversions extends Serializable {
  implicit def dslToOutputT[F[_], Er, A, B: ClassTag, E <: Environment, R0[_]](dsl: foldLeftDsl[A, B])(
      implicit canFold: CanFold.Aux[E#Repr, R0],
      arrow: R0 ~> F
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, B]]]] = dsl[F, Er, E](canFold)(arrow)

  implicit def dslToOutputApplicative[F[_]: Applicative, Er, A, B: ClassTag, E <: Environment](dsl: foldLeftDsl[A, B])(
      implicit canFold: CanFold.Aux[E#Repr, Id]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, B]]]] = dsl[F, Er, E](canFold)(idTo[F])
}

object foldLeftDsl extends LowPriorityFoldLeftConversions

class foldFDsl(val `dummy`: Boolean = true) extends AnyVal {
  def failOnError[F[_], Er, A, B](zero: B)(f: (B, A) => F[B]): foldWithErrorsFDsl[F, Er, A, B] =
    new foldWithErrorsFDsl[F, Er, A, B](zero -> f)
}

class foldWithErrorsFDsl[F[_], Er, A, B](val `this`: (B, (B, A) => F[B])) extends AnyVal {
  def apply[E <: Environment](implicit canFold: CanFoldF[E#Repr, F], B: ClassTag[B]) =
    new FoldFOutput[F, Er, A, B, E](`this`._1)(`this`._2)(canFold)
}

object foldWithErrorsFDsl {
  implicit def dslToOutputF[F[_], Er, A, B: ClassTag, E <: Environment](dsl: foldWithErrorsFDsl[F, Er, A, B])(
      implicit canFold: CanFoldF[E#Repr, F]
  ): OutputT.Aux[F, Er, A, E, λ[(G[_], β) => G[Either[Er, B]]]] = dsl[E]
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

object sizeDsl extends LowPrioritySizeConversions
