package com.github.trembita.experimental.spark.streaming

import cats.effect.IO
import cats.{Monad, MonadError}
import com.github.trembita._
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait MagnetlessSparkStreamingBasicOps[F[_], A] extends Any {
  def `this`: DataPipelineT[F, A, SparkStreaming]

  def map[B: ClassTag](
      f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, SparkStreaming] =
    `this`.mapImpl(f)

  def mapConcat[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, SparkStreaming] =
    `this`.mapConcatImpl(f)

  def filter(p: A => Boolean)(implicit F: Monad[F], A: ClassTag[A]): DataPipelineT[F, A, SparkStreaming] =
    `this`.filterImpl(p)

  def collect[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, SparkStreaming] =
    `this`.collectImpl(pf)

  def flatCollect[B: ClassTag](
      pf: PartialFunction[A, Iterable[B]]
  )(implicit F: Monad[F]): DataPipelineT[F, B, SparkStreaming] =
    `this`.collectImpl(pf).flatten

  def handleError(f: Throwable => A)(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): DataPipelineT[F, A, SparkStreaming] =
    `this`.handleErrorImpl[A](f)

  def recover(pf: PartialFunction[Throwable, A])(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): DataPipelineT[F, A, SparkStreaming] =
    `this`.handleErrorImpl[A](pf.applyOrElse(_, (e: Throwable) => throw e))

  def recoverNonFatal(f: Throwable => A)(
      implicit F: MonadError[F, Throwable],
      A: ClassTag[A]
  ): DataPipelineT[F, A, SparkStreaming] =
    `this`.handleErrorImpl {
      case NonFatal(e) => f(e)
      case other       => throw other
    }

}

trait MagnetlessSparkStreamingIOOps[A] extends Any {
  def `this`: DataPipelineT[IO, A, SparkStreaming]

  def mapM[B: ClassTag](f: A => IO[B]): DataPipelineT[IO, B, SparkStreaming] =
    `this`.mapMImpl(f)

  def handleErrorWith(
      f: Throwable => IO[A]
  )(implicit A: ClassTag[A]): DataPipelineT[IO, A, SparkStreaming] =
    `this`.handleErrorWithImpl(f)

  def recoverWith(
      pf: PartialFunction[Throwable, IO[A]]
  )(implicit A: ClassTag[A]): DataPipelineT[IO, A, SparkStreaming] =
    `this`.handleErrorWithImpl(
      pf.applyOrElse(_, (e: Throwable) => IO.raiseError(e))
    )
}