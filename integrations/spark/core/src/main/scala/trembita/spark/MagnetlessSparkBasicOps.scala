package trembita.spark

import cats.effect.IO
import cats.{Monad, MonadError}
import trembita._

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait MagnetlessSparkBasicOps[F[_], A, E <: BaseSpark] extends Any {
  def `this`: BiDataPipelineT[F, A, E]

  def map[B: ClassTag](
      f: A => B
  )(implicit F: Monad[F]): BiDataPipelineT[F, B, E] =
    `this`.mapImpl(f)

  def mapConcat[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): BiDataPipelineT[F, B, E] =
    `this`.mapConcatImpl(f)

  def filter(p: A => Boolean)(implicit F: Monad[F], A: ClassTag[A]): BiDataPipelineT[F, A, E] =
    `this`.filterImpl(p)

  def collect[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): BiDataPipelineT[F, B, E] =
    `this`.collectImpl(pf)

  def flatCollect[B: ClassTag](
      pf: PartialFunction[A, Iterable[B]]
  )(implicit F: Monad[F]): BiDataPipelineT[F, B, E] =
    `this`.collectImpl(pf).flatten

  def handleError(f: Throwable => A)(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): BiDataPipelineT[F, A, E] =
    `this`.handleErrorImpl[A](f)

  def recover(pf: PartialFunction[Throwable, A])(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): BiDataPipelineT[F, A, E] =
    `this`.handleErrorImpl[A](pf.applyOrElse(_, (e: Throwable) => throw e))

  def recoverNonFatal(f: Throwable => A)(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): BiDataPipelineT[F, A, E] =
    `this`.handleErrorImpl {
      case NonFatal(e) => f(e)
      case other       => throw other
    }
}

trait MagnetlessSparkIOOps[A, E <: BaseSpark] extends Any {
  def `this`: BiDataPipelineT[IO, A, E]

  def handleErrorWith(
      f: Throwable => IO[A]
  )(implicit A: ClassTag[A]): BiDataPipelineT[IO, A, E] =
    `this`.handleErrorWithImpl(f)

  def recoverWith(
      pf: PartialFunction[Throwable, IO[A]]
  )(implicit A: ClassTag[A]): BiDataPipelineT[IO, A, E] =
    `this`.handleErrorWithImpl(
      pf.applyOrElse(_, (e: Throwable) => IO.raiseError(e))
    )
}
