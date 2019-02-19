package trembita

import scala.language.higherKinds
import scalaz.zio._
import scalaz.zio.interop.catz._

package object zio {
  type BiDataPipelineT[F[_, _], A, E, Env <: Environment] = BiDataPipelineT[F[E, ?], A, Env]
}
