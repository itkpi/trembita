package trembita.akka_streams

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.{~>, Applicative, Id, Monad}
import trembita._
import trembita.operations.{CanFold, HasSize}
import trembita.outputs.Keep
import trembita.outputs.internal._
import scala.collection.generic.CanBuildFrom
import scala.language.{existentials, higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait OutputDslAkka[F[_], A, Mat] extends Any with Serializable {
  def `this`: BiDataPipelineT[F, A, Akka[Mat]]

  @inline def into(output: OutputWithPropsT[F, Akka[Mat]]): outputWithPropsDsl[F, A, Mat, output.Props, output.Out] =
    new outputWithPropsDsl[F, A, Mat, output.Props, output.Out](
      (`this`, output.widen)
    )

  @inline def into(
      output: OutputT[F, A, Akka[Mat]]
  ): outputWithoutPropsDsl[F, A, Mat, output.Out] =
    new outputWithoutPropsDsl[F, A, Mat, output.Out](
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
class outputWithPropsDsl[F[_], A, Mat, P0[_], Out0[_[_], _]](
    val `this`: (BiDataPipelineT[F, A, Akka[Mat]], OutputWithPropsT.Aux[F, Akka[Mat], P0, Out0])
) extends AnyVal
    with Serializable {
  @inline def run(implicit props: P0[A], E: Akka[Mat], run: Akka[Mat]#Run[F], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    `this`._2[A](props)(`this`._1)

  @inline def run(props: P0[A])(implicit _run: Akka[Mat]#Run[F], E: Akka[Mat], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    run(props, E, _run, F, A)

  @inline def alsoInto(
      output: OutputWithPropsT[F, Akka[Mat]]
  ): keepDslWithProps[F, A, Mat, P0, Out0, output.Props, output.Out] = new keepDslWithProps[F, A, Mat, P0, Out0, output.Props, output.Out](
    (`this`._1, `this`._2, output.widen)
  )

  @inline def alsoInto(
      output: OutputT[F, A, Akka[Mat]]
  ): keepDslCombinedLeft[F, A, Mat, P0, Out0, output.Out] = new keepDslCombinedLeft[F, A, Mat, P0, Out0, output.Out](
    (`this`._1, `this`._2, output.widen)
  )
}

class keepDslWithProps[F[_], A, Mat, P0[_], Out0[_[_], _], P1[_], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, A, Akka[Mat]], OutputWithPropsT.Aux[F, Akka[Mat], P0, Out0], OutputWithPropsT.Aux[F, Akka[Mat], P1, Out1])
) extends AnyVal
    with Serializable {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, A, Mat, λ[β => (P0[β], P1[A])], Out0] =
    new outputWithPropsDsl[F, A, Mat, λ[β => (P0[β], P1[A])], Out0](
      (
        `this`._1,
        Keep
          .left[Out0, Out1]
          .newOutputWithProps[F, Akka[Mat], P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, Akka[Mat], λ[β => (P0[β], P1[A])], Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, A, Mat, λ[β => (P0[β], P1[A])], Out1] =
    new outputWithPropsDsl[F, A, Mat, λ[β => (P0[β], P1[A])], Out1](
      (
        `this`._1,
        Keep
          .right[Out0, Out1]
          .newOutputWithProps[F, Akka[Mat], P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, Akka[Mat], λ[β => (P0[β], P1[A])], Out1]]
      )
    )

  @inline def keepBoth: outputWithPropsDsl[F, A, Mat, λ[β => (P0[β], P1[A])], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] =
    new outputWithPropsDsl[F, A, Mat, λ[β => (P0[β], P1[A])], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
      (
        `this`._1,
        Keep
          .both[Out0, Out1]
          .newOutputWithProps[F, Akka[Mat], P0, P1](`this`._2, `this`._3)
          .asInstanceOf[OutputWithPropsT.Aux[F, Akka[Mat], λ[β => (P0[β], P1[A])], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]]]
      )
    )

  @inline def keepMat(implicit materializer: Materializer): outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], a) => G[Mat]]] =
    new outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], a) => G[Mat]]]((`this`._1, {
      new OutputWithPropsT[F, Akka[Mat]] {
        final type Out[G[_], a] = G[Mat]
        final type Props[a]     = P0[a]
        def apply[Ax: ClassTag](props: Props[Ax])(
            pipeline: BiDataPipelineT[F, Ax, Akka[Mat]]
        )(implicit F: Monad[F], E: Akka[Mat], run: RunAkka[F]): F[Mat] =
          F.map(pipeline.evalRepr)(_.toMat(Sink.ignore)(akka.stream.scaladsl.Keep.left).run())
      }
    }))
}

class outputWithoutPropsDsl[F[_], A, Mat, Out0[_[_], _]](
    val `this`: (BiDataPipelineT[F, A, Akka[Mat]], OutputT.Aux[F, A, Akka[Mat], Out0])
) extends AnyVal
    with Serializable {
  @inline def run(implicit E: Akka[Mat], run: Akka[Mat]#Run[F], F: Monad[F], A: ClassTag[A]): Out0[F, A] =
    `this`._2(`this`._1)

  @inline def alsoInto(output: OutputT[F, A, Akka[Mat]]): keepDslWithoutProps[F, A, Mat, Out0, output.Out] =
    new keepDslWithoutProps[F, A, Mat, Out0, output.Out](
      (`this`._1, `this`._2, output.widen)
    )

  @inline def alsoInto(
      output: OutputWithPropsT[F, Akka[Mat]]
  ): keepDslCombinedRight[F, A, Mat, output.Props, output.Out, Out0] = new keepDslCombinedRight[F, A, Mat, output.Props, output.Out, Out0](
    (`this`._1, output.widen, `this`._2)
  )

  @inline def keepMat(implicit materializer: Materializer): outputWithoutPropsDsl[F, A, Mat, λ[(G[_], a) => G[Mat]]] =
    new outputWithoutPropsDsl[F, A, Mat, λ[(G[_], a) => G[Mat]]]((`this`._1, {
      new OutputT[F, A, Akka[Mat]] {
        type Out[G[_], a] = G[Mat]
        def apply(pipeline: BiDataPipelineT[F, A, Akka[Mat]])(
            implicit F: Monad[F],
            E: Akka[Mat],
            run: RunAkka[F],
            A: ClassTag[A]
        ): F[Mat] = F.map(pipeline.evalRepr)(_.toMat(Sink.ignore)(akka.stream.scaladsl.Keep.left).run())
      }
    }))
}

class keepDslCombinedLeft[F[_], A, Mat, P0[_], Out0[_[_], _], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, A, Akka[Mat]], OutputWithPropsT.Aux[F, Akka[Mat], P0, Out0], OutputT.Aux[F, A, Akka[Mat], Out1])
) extends AnyVal
    with Serializable {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, A, Mat, P0, Out0] =
    new outputWithPropsDsl[F, A, Mat, P0, Out0](
      (
        `this`._1,
        CombinedOutput(`this`._2.widen, Keep.left[Out0, Out1], `this`._3.widen)
          .asInstanceOf[OutputWithPropsT.Aux[F, Akka[Mat], P0, Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, A, Mat, P0, Out1] =
    new outputWithPropsDsl[F, A, Mat, P0, Out1](
      (
        `this`._1,
        CombinedOutput(`this`._2.widen, Keep.right[Out0, Out1], `this`._3.widen)
          .asInstanceOf[OutputWithPropsT.Aux[F, Akka[Mat], P0, Out1]]
      )
    )

  @inline def keepBoth: outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] =
    new outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
      (
        `this`._1,
        CombinedOutput[F, A, Akka[Mat], P0, Out0, Out1, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
          `this`._2.widen,
          Keep.both[Out0, Out1],
          `this`._3.widen
        )
      )
    )

  @inline def ignoreBoth(
      implicit ev0: Out0[F, _] <:< F[_],
      ev1: Out1[F, _] <:< F[_]
  ): outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], A) => G[Unit]]] = new outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], A) => G[Unit]]](
    (
      `this`._1, {
        val keepBothOutput =
          CombinedOutput[F, A, Akka[Mat], P0, Out0, Out1, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
            `this`._2,
            Keep.both[Out0, Out1],
            `this`._3
          )

        val keepBindOutput =
          new OutputWithPropsT[F, Akka[Mat]] {
            type Props[β]     = P0[β]
            type Out[G[_], β] = G[Unit]
            def apply[Ax: ClassTag](props: Props[Ax])(
                pipeline: BiDataPipelineT[F, Ax, Akka[Mat]]
            )(implicit F: Monad[F], E: Akka[Mat], run: Akka[Mat]#Run[F]): F[Unit] = {
              val (left, right) = keepBothOutput[Ax](props)(pipeline)
              F.productR(
                ev0(left)
              )(
                F.void(
                  ev1(right)
                )
              )
            }
          }.asInstanceOf[OutputWithPropsT.Aux[F, Akka[Mat], P0, λ[(G[_], A) => G[Unit]]]]

        keepBindOutput
      }
    )
  )
}

class keepDslCombinedRight[F[_], A, Mat, P0[_], Out0[_[_], _], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, A, Akka[Mat]], OutputWithPropsT.Aux[F, Akka[Mat], P0, Out0], OutputT.Aux[F, A, Akka[Mat], Out1])
) extends AnyVal
    with Serializable {
  @inline private def leftDsl =
    new keepDslCombinedLeft[F, A, Mat, P0, Out0, Out1](
      `this`
    )

  @inline def keepLeft(implicit keepRight: KeepRight[Out0, Out1]): outputWithPropsDsl[F, A, Mat, P0, Out1] = leftDsl.keepRight

  @inline def keepRight(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithPropsDsl[F, A, Mat, P0, Out0] = leftDsl.keepLeft

  @inline def keepBoth: outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] = leftDsl.keepBoth

  @inline def ignoreBoth(
      implicit ev0: Out0[F, _] <:< F[_],
      ev1: Out1[F, _] <:< F[_]
  ): outputWithPropsDsl[F, A, Mat, P0, λ[(G[_], A) => G[Unit]]] = leftDsl.ignoreBoth
}

class keepDslWithoutProps[F[_], A, Mat, Out0[_[_], _], Out1[_[_], _]](
    val `this`: (BiDataPipelineT[F, A, Akka[Mat]], OutputT.Aux[F, A, Akka[Mat], Out0], OutputT.Aux[F, A, Akka[Mat], Out1])
) extends AnyVal {
  @inline def keepLeft(implicit keepLeft: KeepLeft[Out0, Out1]): outputWithoutPropsDsl[F, A, Mat, Out0] =
    new outputWithoutPropsDsl[F, A, Mat, Out0](
      (
        `this`._1,
        Keep
          .left[Out0, Out1]
          .newOutputWithoutProps[F, A, Akka[Mat]](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, A, Akka[Mat], Out0]]
      )
    )

  @inline def keepRight(implicit keepRight: KeepRight[Out0, Out1]): outputWithoutPropsDsl[F, A, Mat, Out1] =
    new outputWithoutPropsDsl[F, A, Mat, Out1](
      (
        `this`._1,
        Keep
          .right[Out0, Out1]
          .newOutputWithoutProps[F, A, Akka[Mat]](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, A, Akka[Mat], Out1]]
      )
    )

  @inline def keepBoth: outputWithoutPropsDsl[F, A, Mat, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]] =
    new outputWithoutPropsDsl[F, A, Mat, λ[(G[_], β) => (Out0[G, β], Out1[G, β])]](
      (
        `this`._1,
        Keep
          .both[Out0, Out1]
          .newOutputWithoutProps[F, A, Akka[Mat]](`this`._2, `this`._3)
          .asInstanceOf[OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => (Out0[G, β], Out1[G, β])]]]
      )
    )

  @inline def ignoreBoth[Tx, Ux](
      implicit ev0: Out0[F, A] <:< F[Tx],
      ev1: Out1[F, A] <:< F[Ux]
  ): outputWithoutPropsDsl[F, A, Mat, λ[(G[_], A) => G[Unit]]] = new outputWithoutPropsDsl[F, A, Mat, λ[(G[_], A) => G[Unit]]](
    (
      `this`._1, {
        val keepIgnore = new OutputT[F, A, Akka[Mat]] {
          type Out[G[_], β] = G[Unit]

          def apply(
              pipeline: BiDataPipelineT[F, A, Akka[Mat]]
          )(implicit F: Monad[F], E: Akka[Mat], run: Akka[Mat]#Run[F], A: ClassTag[A]): F[Unit] = {
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
        }.asInstanceOf[OutputT.Aux[F, A, Akka[Mat], λ[(G[_], A) => G[Unit]]]]
        keepIgnore
      }
    )
  )
}
//
//class collectionDsl[Col[x] <: Iterable[x]](val `dummy`: Boolean = true) extends AnyVal with Serializable
//
//object collectionDsl {
//  implicit def dslToSeqOutput[F[_], Col[x] <: Iterable[x]](
//      dsl: collectionDsl[Col]
//  ): OutputWithPropsT.Aux[F, Sequential, λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
//    new SequentialCollectionOutput[Col, F]()
//
//  implicit def dslToParOutput[F[_], Col[x] <: Iterable[x]](
//      dsl: collectionDsl[Col]
//  ): OutputWithPropsT.Aux[F, Parallel, λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
//    new ParallelCollectionOutput[Col, F]()
//}
//
//class foreachDsl[A](val `f`: A => Unit) extends AnyVal with Serializable {
//  @inline def apply[F[_], Mat]() =
//    new ForeachOutput[F, A, Akka[Mat]](`f`)
//}
//
//trait LowPriorityForeachConversions extends Serializable {
//  implicit def dslToOutputT[F[_], A, Mat](dsl: foreachDsl[A]): OutputT.Aux[F, A, Akka[Mat], λ[(F[_], β) => F[Unit]]] =
//    dsl()
//}
//
//object foreachDsl extends LowPriorityForeachConversions {
//  implicit def dslToOutputId[A, Mat](dsl: foreachDsl[A]): OutputT.Aux[Id, A, Akka[Mat], λ[(F[_], β) => F[Unit]]] =
//    dsl()
//}
//
//class reduceDsl[A](val `f`: (A, A) => A) extends AnyVal with Serializable {
//  @inline def apply[F[_], Mat](ev: CanFold[Akka[Mat]#Repr])(arrow: ev.Result ~> F) =
//    new ReduceOutput[F, A, Akka[Mat], ev.Result](`f`)(ev)(arrow)
//}
//
//trait LowPriorityReduceConversions extends Serializable {
//  implicit def dslToOutput[F[_], A, Mat, R0[_]](dsl: reduceDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, R0],
//      arrow: R0 ~> F
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[β]]] = dsl[F, Mat](canFold)(arrow)
//
//  implicit def dslToOutputApplicative[F[_]: Applicative, A, Mat](dsl: reduceDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[β]]] = dsl[F, Mat](canFold)(idTo[F])
//}
//
//object reduceDsl extends LowPriorityReduceConversions {
//  implicit def dslToOutputId[A, Mat](dsl: reduceDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], β) => G[β]]] = dsl[Id, Mat](canFold)(identityK[Id])
//}
//
//class reduceOptDsl[A](val `f`: (A, A) => A) extends AnyVal with Serializable {
//  @inline def apply[F[_], Mat](ev: CanFold[Akka[Mat]#Repr])(arrow: ev.Result ~> F) =
//    new ReduceOptOutput[F, A, Akka[Mat], ev.Result](`f`)(ev)(arrow)
//}
//
//trait LowPriorityReduceOptDsl extends Serializable {
//  implicit def dslToOutputT[F[_], A, Mat, R0[_]](dsl: reduceOptDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, R0],
//      arrow: R0 ~> F
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[Option[β]]]] = dsl[F, Mat](canFold)(arrow)
//
//  implicit def dslToOutputApplicative[F[_]: Applicative, A, Mat](dsl: reduceOptDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[Option[β]]]] = dsl[F, Mat](canFold)(idTo)
//}
//
//object reduceOptDsl extends LowPriorityReduceOptDsl {
//  implicit def dslToOutputId[A, Mat](dsl: reduceOptDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], β) => G[Option[β]]]] = dsl[Id, Mat](canFold)(identityK[Id])
//}
//
//class foldDsl[A](val `this`: (A, (A, A) => A)) extends AnyVal with Serializable {
//  @inline def apply[F[_], Mat](ev: CanFold[Akka[Mat]#Repr])(arrow: ev.Result ~> F) =
//    new FoldOutput[F, A, Akka[Mat], ev.Result](`this`._1)(`this`._2)(ev)(arrow)
//}
//
//trait LowPriorityFoldConversions extends Serializable {
//  implicit def dslToOutputT[F[_], A, Mat, R0[_]](dsl: foldDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, R0],
//      arrow: R0 ~> F
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[β]]] = dsl[F, Mat](canFold)(arrow)
//
//  implicit def dslToOutputApplicative[F[_]: Applicative, A, Mat](dsl: foldDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[β]]] = dsl[F, Mat](canFold)(idTo[F])
//}
//
//object foldDsl extends LowPriorityFoldConversions {
//  implicit def dslToOutputId[A, Mat](dsl: foldDsl[A])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], β) => G[β]]] = dsl[Id, Mat](canFold)(identityK[Id])
//}
//
//class foldLeftDsl[A, B](val `this`: (B, (B, A) => B)) extends AnyVal with Serializable {
//  @inline def apply[F[_], Mat](ev: CanFold[Akka[Mat]#Repr])(arrow: ev.Result ~> F)(implicit B: ClassTag[B]) =
//    new FoldLeftOutput[F, A, B, Akka[Mat], ev.Result](`this`._1)(`this`._2)(ev)(arrow)
//}
//
//trait LowPriorityFoldLeftConversions extends Serializable {
//  implicit def dslToOutputT[F[_], A, B: ClassTag, Mat, R0[_]](dsl: foldLeftDsl[A, B])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, R0],
//      arrow: R0 ~> F
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[B]]] = dsl[F, Mat](canFold)(arrow)
//
//  implicit def dslToOutputApplicative[F[_]: Applicative, A, B: ClassTag, Mat](dsl: foldLeftDsl[A, B])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[B]]] = dsl[F, Mat](canFold)(idTo[F])
//}
//
//object foldLeftDsl extends LowPriorityFoldLeftConversions {
//  implicit def dslToOutputId[A, B: ClassTag, Mat](dsl: foldLeftDsl[A, B])(
//      implicit canFold: CanFold.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], β) => G[B]]] = dsl[Id, Mat](canFold)(identityK[Id])
//}
//
//class sizeDsl(val `dummy`: Boolean = true) extends AnyVal with Serializable {
//  def apply[F[_], A, Mat](ev: HasSize[Akka[Mat]#Repr])(arrow: ev.Result ~> F) =
//    new SizeOutput[F, A, Akka[Mat], ev.Result](ev)(arrow)
//}
//
//trait LowPrioritySizeConversions extends Serializable {
//  implicit def dslToOutputT[F[_], A, Mat, R0[_]](dsl: sizeDsl)(
//      implicit hasSize: HasSize.Aux[Akka[Mat]#Repr, R0],
//      arrow: R0 ~> F
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[Int]]] =
//    dsl[F, A, Mat](hasSize)(arrow)
//
//  implicit def dslToOutputApplicative[F[_]: Applicative, A, Mat, R0[_]](dsl: sizeDsl)(
//      implicit hasSize: HasSize.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[F, A, Akka[Mat], λ[(G[_], β) => G[Int]]] =
//    dsl[F, A, Mat](hasSize)(idTo[F])
//}
//
//object sizeDsl extends LowPrioritySizeConversions {
//  implicit def dslToOutputId[A, Mat, R0[_]](dsl: sizeDsl)(
//      implicit hasSize: HasSize.Aux[Akka[Mat]#Repr, Id]
//  ): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], β) => G[Int]]] =
//    dsl[Id, A, Mat](hasSize)(identityK[Id])
//}
