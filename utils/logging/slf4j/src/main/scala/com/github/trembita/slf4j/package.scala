package com.github.trembita

import cats.Applicative
import cats.effect.Sync
import com.github.trembita.logging.commons.LoggingF
import org.slf4j.Logger
import scala.language.higherKinds

package object slf4j {
  def mkLogging[F[_]](logger: Logger)(implicit F: Sync[F]): LoggingF[F] = new LoggingF[F] {
    override def info(msg: String): F[Unit]                = F.delay(logger.info(msg))
    override def warn(msg: String): F[Unit]                = F.delay(logger.warn(msg))
    override def trace(msg: String): F[Unit]               = F.delay(logger.trace(msg))
    override def debug(msg: String): F[Unit]               = F.delay(logger.debug(msg))
    override def error(msg: String, e: Throwable): F[Unit] = F.delay(logger.error(msg, e))
  }

  def mkLoggingImpure[F[_]](logger: Logger)(implicit F: Applicative[F]): LoggingF[F] = new LoggingF[F] {
    override def info(msg: String): F[Unit]                = F.pure(logger.info(msg))
    override def warn(msg: String): F[Unit]                = F.pure(logger.warn(msg))
    override def trace(msg: String): F[Unit]               = F.pure(logger.trace(msg))
    override def debug(msg: String): F[Unit]               = F.pure(logger.debug(msg))
    override def error(msg: String, e: Throwable): F[Unit] = F.pure(logger.error(msg, e))
  }
}
