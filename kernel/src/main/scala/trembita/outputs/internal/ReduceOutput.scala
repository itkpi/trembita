package trembita.outputs.internal

import cats.{~>, Monad, MonadError}
import trembita._
import trembita.operations._

import scala.reflect.ClassTag
import scala.language.higherKinds

class ReduceOutput[F[_], Er, A, E <: Environment, R0[_]](f: (A, A) => A)(
    canFold: CanReduce.Aux[E#Repr, R0]
)(arrow: R0 ~> F)
    extends OutputT[F, Er, A, E] {
  final type Out[G[_], β] = G[Either[Er, β]]

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Either[Er, AA]] =
    F.flatMap(pipeline.evalRepr)(
      repr =>
        arrow(canFold.reduce(repr) {
          case (acc @ Left(_), _)     => acc
          case (_, Left(er))          => Left(er)
          case (Right(acc), Right(v)) => Right(f(acc.asInstanceOf[A], v.asInstanceOf[A]))
        }).asInstanceOf[F[Either[Er, AA]]]
    )
}

class ReduceOptOutput[F[_], Er, A, E <: Environment, R0[_]](f: (A, A) => A)(
    canFold: CanReduce.Aux[E#Repr, R0]
)(arrow: R0 ~> F)
    extends OutputT[F, Er, A, E] {
  type Out[G[_], β] = G[Option[Either[Er, β]]]

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Option[Either[Er, AA]]] =
    F.flatMap(pipeline.evalRepr)(
      repr =>
        arrow(canFold.reduceOpt(repr) {
          case (acc @ Left(_), _)     => acc
          case (_, Left(er))          => Left(er)
          case (Right(acc), Right(v)) => Right(f(acc.asInstanceOf[A], v.asInstanceOf[A]))
        }).asInstanceOf[F[Option[Either[Er, AA]]]]
    )
}

class FoldOutput[F[_], Er, A, E <: Environment, R0[_]](zero: A)(f: (A, A) => A)(
    canFold: CanFold.Aux[E#Repr, R0]
)(arrow: R0 ~> F)
    extends OutputT[F, Er, A, E] {
  type Out[G[_], β] = G[Either[Er, β]]

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Either[Er, AA]] =
    F.flatMap(pipeline.evalRepr)(
      repr =>
        arrow(canFold.foldLeft(repr)(Right(zero): Either[Err, AA]) { (acc, v) =>
          for {
            acc <- acc.right
            v   <- v.right
          } yield f(acc.asInstanceOf[A], v.asInstanceOf[A])
        }).asInstanceOf[F[Either[Er, AA]]]
    )
}

class FoldLeftOutput[
    F[_],
    Er,
    A,
    B: ClassTag,
    E <: Environment,
    R0[_]
](zero: B)(f: (B, A) => B)(canFold: CanFold.Aux[E#Repr, R0])(
    arrow: R0 ~> F
) extends OutputT[F, Er, A, E] {
  type Out[G[_], β] = G[Either[Er, B]]

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Either[Er, B]] =
    F.flatMap(pipeline.evalRepr)(
      repr =>
        arrow(canFold.foldLeft(repr)(Right(zero): Either[Er, B]) {
          case (acc @ Left(_), _)     => acc
          case (_, Left(er))          => Left(er.asInstanceOf[Er])
          case (Right(acc), Right(v)) => Right(f(acc, v.asInstanceOf[A]))
        })
    )
}

class SizeOutput[F[_], Er, A, E <: Environment, R0[_]](hasSize: HasSize.Aux[E#Repr, R0])(
    arrow: R0 ~> F
) extends OutputT[F, Er, A, E] {
  type Out[G[_], β] = G[Int]

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Int] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(hasSize.size(repr)))
}

class SizeOutput2[F[_], Er, A, E <: Environment, R0[_]](hasSize: HasBigSize.Aux[E#Repr, R0])(
    arrow: R0 ~> F
) extends OutputT[F, Er, A, E] {
  type Out[G[_], β] = G[Long]

  def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Long] =
    F.flatMap(pipeline.evalRepr)(repr => arrow(hasSize.size(repr)))
}

class FoldFOutput[F[_], Er, A, B: ClassTag, E <: Environment](
    zero: B
)(f: (B, A) => F[B])(canFold: CanFoldF[E#Repr, F])
    extends OutputT[F, Er, A, E] {
  type Out[G[_], b] = G[Either[Er, B]]
  override def apply[Err >: Er, AA >: A](
      pipeline: BiDataPipelineT[F, Err, AA, E]
  )(implicit F: MonadError[F, Er], E: E, run: E#Run[F], A: ClassTag[AA]): F[Either[Er, B]] =
    F.flatMap(pipeline.evalRepr) { repr =>
      canFold.foldF[Either[Err, AA], Either[Er, B]](repr)(Right(zero): Either[Er, B]) {
        case (acc @ Left(_), _)     => F.pure(acc)
        case (_, Left(er))          => F.pure(Left[Er, B](er.asInstanceOf[Er]))
        case (Right(acc), Right(v)) => F.map(f(acc, v.asInstanceOf[A]))(Right(_))
      }
    }
}
