package com.github.trembita

import org.slf4j.Logger
import scala.language.higherKinds

package object slf4j {
  implicit class LoggingOps[F[_], A, Ex <: Execution](
    private val self: DataPipelineT[F, A, Ex]
  ) extends AnyVal {
    def withLogger(logger: Logger): DataPipelineT[F, A, Ex] =
      LoggedSource(logger)(self)
  }
}
