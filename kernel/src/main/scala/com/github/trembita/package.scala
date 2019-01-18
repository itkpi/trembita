package com.github

import scala.language.{higherKinds, implicitConversions}
import cats._
import com.github.trembita.internal._
import com.github.trembita.operations._
import com.github.trembita.outputs.internal.{lowPriorityTricks, standardOutputs, OutputDsl}
import scala.reflect.ClassTag

package object trembita extends standardMagnets with arrows with standardOutputs with lowPriorityTricks {

  type DataPipeline[A, E <: Environment] = DataPipelineT[Id, A, E]

  implicit class CommonOps[F[_], A, E <: Environment](
      val `this`: DataPipelineT[F, A, E]
  ) extends AnyVal
      with EnvironmentIndependentOps[F, A, E]
      with EnvironmentDependentOps[F, A, E]
      with OutputDsl[F, A, E]

  implicit class SeqOps[F[_], A](val `this`: DataPipelineT[F, A, Sequential]) extends AnyVal with MagnetlessOps[F, A, Sequential]

  implicit class ParOps[F[_], A](val `this`: DataPipelineT[F, A, Parallel]) extends AnyVal with MagnetlessOps[F, A, Parallel]

  type PairPipelineT[F[_], K, V, Ex <: Environment] =
    DataPipelineT[F, (K, V), Ex]

  /**
    * Operations for [[DataPipelineT]] of tuples
    * (NOT [[MapPipelineT]])
    **/
  implicit class PairPipelineOps[F[_], K, V, Ex <: Environment](
      val self: PairPipelineT[F, K, V, Ex]
  ) extends AnyVal {
    def mapValues[W](
        f: V => W
    )(implicit F: Monad[F]): PairPipelineT[F, K, W, Ex] = self.mapImpl {
      case (k, v) => (k, f(v))
    }

    def keys(implicit F: Monad[F], K: ClassTag[K]): DataPipelineT[F, K, Ex] =
      self.mapImpl(_._1)

    def values(implicit F: Monad[F], V: ClassTag[V]): DataPipelineT[F, V, Ex] =
      self.mapImpl(_._2)

    /** @return - [[MapPipelineT]] */
    def toMapPipeline(implicit K: ClassTag[K], V: ClassTag[V], F: Monad[F]): MapPipelineT[F, K, V, Ex] =
      new BaseMapPipelineT[F, K, V, Ex](
        self.asInstanceOf[DataPipelineT[F, (K, V), Ex]],
        F
      )
  }

  type Sequential = Environment.Sequential
  type Parallel   = Environment.Parallel
}
