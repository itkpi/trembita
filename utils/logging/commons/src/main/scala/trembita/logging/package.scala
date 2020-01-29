package trembita

import trembita.logging.commons.LoggingF
import cats.{Monad, MonadError}
import cats.syntax.all._
import scala.language.higherKinds
import scala.reflect.ClassTag

package object logging {
  implicit class LoggingOps[F[_], A, E <: Environment](private val self: DataPipelineT[F, A, E]) extends AnyVal {
    def info(msg: A => String = (a: A) => a.toString)(implicit F: Monad[F], loggingF: LoggingF[F], A: ClassTag[A]): DataPipelineT[F, A, E] =
      self.mapMImpl[A, A] { a =>
        loggingF.info(msg(a)).as(a)
      }

    def warn(msg: A => String = (a: A) => a.toString)(implicit F: Monad[F], loggingF: LoggingF[F], A: ClassTag[A]): DataPipelineT[F, A, E] =
      self.mapMImpl[A, A] { a =>
        loggingF.warn(msg(a)).as(a)
      }

    def debug(
        msg: A => String = (a: A) => a.toString
    )(implicit F: Monad[F], loggingF: LoggingF[F], A: ClassTag[A]): DataPipelineT[F, A, E] =
      self.mapMImpl[A, A] { a =>
        loggingF.debug(msg(a)).as(a)
      }

    def trace(
        msg: A => String = (a: A) => a.toString
    )(implicit F: Monad[F], loggingF: LoggingF[F], A: ClassTag[A]): DataPipelineT[F, A, E] =
      self.mapMImpl[A, A] { a =>
        loggingF.trace(msg(a)).as(a)
      }

    def logErrors(msg: => String)(implicit F: MonadError[F, Throwable], loggingF: LoggingF[F], A: ClassTag[A]): DataPipelineT[F, A, E] =
      self.catchAllWith[A] { e =>
        loggingF.error(msg, e) *> F.raiseError(e)
      }
  }
}
