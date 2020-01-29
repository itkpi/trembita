package trembita.spark

import cats.effect.IO
import cats.{Monad, MonadError}
import trembita._

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait MagnetlessSparkBasicOps[F[_], A, E <: BaseSpark] extends Any {
  def `this`: DataPipelineT[F, A, E]

  def map[B: ClassTag](
      f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, E] =
    `this`.mapImpl(f)

  def mapConcat[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, E] =
    `this`.mapConcatImpl(f)

  def filter(p: A => Boolean)(implicit F: Monad[F], A: ClassTag[A]): DataPipelineT[F, A, E] =
    `this`.filterImpl(p)

  def collect[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, E] =
    `this`.collectImpl(pf)

  def flatCollect[B: ClassTag](
      pf: PartialFunction[A, Iterable[B]]
  )(implicit F: Monad[F]): DataPipelineT[F, B, E] =
    `this`.collectImpl(pf).flatten

  def handleError(f: Throwable => A)(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): DataPipelineT[F, A, E] =
    `this`.catchAll[A](f)

  def recover(pf: PartialFunction[Throwable, A])(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): DataPipelineT[F, A, E] =
    `this`.catchAll[A](pf.applyOrElse(_, (e: Throwable) => throw e))

  def recoverNonFatal(f: Throwable => A)(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): DataPipelineT[F, A, E] =
    `this`.catchAll {
      case NonFatal(e) => f(e)
      case other       => throw other
    }
}

trait MagnetlessSparkIOOps[A, E <: BaseSpark] extends Any {
  def `this`: DataPipelineT[IO, A, E]

  def handleErrorWith(
      f: Throwable => IO[A]
  )(implicit A: ClassTag[A]): DataPipelineT[IO, A, E] =
    `this`.catchAllWith(f)

  def recoverWith(
      pf: PartialFunction[Throwable, IO[A]]
  )(implicit A: ClassTag[A]): DataPipelineT[IO, A, E] =
    `this`.catchAllWith(
      pf.applyOrElse(_, (e: Throwable) => IO.raiseError(e))
    )
}
