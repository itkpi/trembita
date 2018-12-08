package com.github.trembita.slf4j

import cats.{Monad, MonadError, ~>}
import org.slf4j.Logger
import com.github.trembita._
import scala.language.higherKinds

protected[trembita] class LoggedSource[F[_], +A, Ex <: Execution](
  logger: Logger,
  source: DataPipelineT[F, A, Ex],
) extends DataPipelineT[F, A, Ex] {

  override def map[B](
    f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.map(f))

  override def flatMap[B](
    f: A => DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.flatMap(f))

  override def filter(
    p: A => Boolean
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    new LoggedSource[F, A, Ex](logger, source.filter(p))

  override def collect[B](
    pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.collect(pf))

  def log[B >: A](
    toString: B => String = (b: B) => b.toString
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] = this.map { a =>
    logger.info(toString(a)); a
  }
  def info[B >: A](toString: A => String = (b: B) => b.toString)(
    implicit F: Monad[F]
  ): DataPipelineT[F, A, Ex] = log(toString)

  def debug[B >: A](
    toString: A => String = (b: B) => b.toString
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    this.map { a =>
      logger.debug(toString(a)); a
    }
  def handleError[B >: A](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.handleError(f))

  def handleErrorWith[B >: A](
    f: Throwable => DataPipelineT[F, B, Ex]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.handleErrorWith(f))

  protected[trembita] def evalFunc[B >: A](Ex: Ex): F[Ex.Repr[B]] =
    source.evalFunc[B](Ex)

  def mapM[B](f: A => F[B])(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.mapM(f))

  def mapK[B, G[_]](
    f: A => G[B]
  )(implicit funcK: G ~> F): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.mapK(f))
}

object LoggedSource {
  def apply[F[_], A, Ex <: Execution](
    logger: Logger
  )(pipeline: DataPipelineT[F, A, Ex]): DataPipelineT[F, A, Ex] =
    new LoggedSource[F, A, Ex](logger, pipeline)
}
