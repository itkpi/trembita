package com.github.trembita.slf4j

import cats.{Monad, MonadError, ~>}
import org.slf4j.Logger
import com.github.trembita._

import scala.language.higherKinds
import scala.reflect.ClassTag

protected[trembita] class LoggedSource[F[_], +A, Ex <: Execution](
  logger: Logger,
  source: DataPipelineT[F, A, Ex],
) extends DataPipelineT[F, A, Ex] {

  override def mapImpl[B: ClassTag](
    f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.mapImpl(f))

  override def flatMapImpl[B: ClassTag](
    f: A => DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.flatMapImpl(f))

  override def filterImpl[AA >: A](
    p: A => Boolean
  )(implicit F: Monad[F], A: ClassTag[AA]): DataPipelineT[F, AA, Ex] =
    new LoggedSource[F, AA, Ex](logger, source.filterImpl[AA](p))

  override def collectImpl[B: ClassTag](
    pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.collectImpl(pf))

  def log[B >: A](
    toString: B => String = (b: B) => b.toString
  )(implicit F: Monad[F], B: ClassTag[B]): DataPipelineT[F, B, Ex] =
    this.mapImpl { a =>
      logger.info(toString(a)); a: B
    }
  def info[B >: A](
    toString: B => String = (b: B) => b.toString
  )(implicit F: Monad[F], B: ClassTag[B]): DataPipelineT[F, B, Ex] =
    this.log[B](toString)

  def debug[B >: A: ClassTag](
    toString: B => String = (b: B) => b.toString
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    this.mapImpl { a =>
      logger.debug(toString(a)); a: B
    }
  def handleErrorImpl[B >: A: ClassTag](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.handleErrorImpl(f))

  override def handleErrorWithImpl[B >: A: ClassTag](
    f: Throwable => F[B]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.handleErrorWithImpl(f))

  protected[trembita] def evalFunc[B >: A](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    source.evalFunc[B](Ex)

  override def mapMImpl[AA >: A, B: ClassTag](
    f: A => F[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new LoggedSource[F, B, Ex](logger, source.mapMImpl[AA, B](f))
}

object LoggedSource {
  def apply[F[_], A, Ex <: Execution](
    logger: Logger
  )(pipeline: DataPipelineT[F, A, Ex]): DataPipelineT[F, A, Ex] =
    new LoggedSource[F, A, Ex](logger, pipeline)
}
