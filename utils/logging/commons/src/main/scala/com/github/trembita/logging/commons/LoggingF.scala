package com.github.trembita.logging.commons
import java.time.LocalDateTime

import cats.Applicative
import scala.language.higherKinds

trait LoggingF[F[_]] {
  def info(msg: String): F[Unit]
  def warn(msg: String): F[Unit]
  def trace(msg: String): F[Unit]
  def debug(msg: String): F[Unit]
  def error(msg: String, e: Throwable): F[Unit]
}

trait LowPriorityLogging {
  implicit def applicativeConsoleLogging[F[_]](implicit F: Applicative[F]): LoggingF[F] = new LoggingF[F] {
    def info(msg: String): F[Unit]                = logInternal("INFO", msg)
    def warn(msg: String): F[Unit]                = logInternal("WARN", msg)
    def trace(msg: String): F[Unit]               = logInternal("TRACE", msg)
    def debug(msg: String): F[Unit]               = logInternal("DEBUG", msg)
    def error(msg: String, e: Throwable): F[Unit] = logInternal("ERROR", msg + s": $e${e.getStackTrace.mkString("\n\t", "\n\t", "\n")}")

    private def logInternal(level: String, msg: String): F[Unit] =
      F.pure(println(s"[$level] - ${LocalDateTime.now} - $msg"))
  }
}

object LoggingF extends LowPriorityLogging {
  def apply[F[_]](implicit ev: LoggingF[F]): LoggingF[F] = ev
}
