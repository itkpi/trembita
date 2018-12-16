package com.github.trembita
import cats.{Monad, MonadError, ~>}

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait MagnetlessOps[F[_], A, Ex <: Execution] extends Any {
  def `this`: DataPipelineT[F, A, Ex]

  def map[B: ClassTag](
    f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapImpl(f)

  def flatMap[B: ClassTag](
    f: A => DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.flatMapImpl(f)

  def filter(p: A => Boolean)(implicit F: Monad[F],
                              A: ClassTag[A]): DataPipelineT[F, A, Ex] =
    `this`.filterImpl(p)

  def collect[B: ClassTag](
    pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.collectImpl(pf)

  def flatCollect[B: ClassTag](
    pf: PartialFunction[A, DataPipelineT[F, B, Ex]]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.collectImpl(pf).flatten

  def handleError(f: Throwable => A)(implicit F: MonadError[F, Throwable],
                                     A: ClassTag[A]): DataPipelineT[F, A, Ex] =
    `this`.handleErrorImpl[A](f)

  def recover(pf: PartialFunction[Throwable, A])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] =
    `this`.handleErrorImpl[A](pf.applyOrElse(_, (e: Throwable) => throw e))

  def recoverNonFatal(f: Throwable => A)(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] =
    `this`.handleErrorImpl {
      case NonFatal(e) => f(e)
      case other       => throw other
    }

  def handleErrorWith(f: Throwable => F[A])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] = `this`.handleErrorWithImpl[A](f)

  def recoverWith(pf: PartialFunction[Throwable, F[A]])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] =
    `this`.handleErrorWithImpl[A](pf.applyOrElse(_, e => F.raiseError[A](e)))

  def mapM[B: ClassTag](
    f: A => F[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapMImpl[A, B](f)

  def mapG[B: ClassTag, G[_]](
    f: A => G[B]
  )(implicit funcK: G ~> F, F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapMImpl[A, B] { a =>
      val gb = f(a)
      val fb = funcK(gb)
      fb
    }
}
