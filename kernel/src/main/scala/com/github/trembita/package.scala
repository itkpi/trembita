package com.github

import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.implicits._
import com.github.trembita.internal._
import scala.reflect.ClassTag

package object trembita extends standardMagnets with arrows with injections {

  type DataPipeline[A, Ex <: Execution] = DataPipelineT[Id, A, Ex]

  implicit class CommonOps[F[_], A, Ex <: Execution](
    val `this`: DataPipelineT[F, A, Ex]
  ) extends AnyVal
      with ExecutionIndependentOps[F, A, Ex] {}

  implicit def liftOps[F[_], A, Ex <: Execution](
    self: DataPipelineT[F, A, Ex]
  )(implicit ex: Ex): ExecutionDependentOps[F, A, Ex] =
    new ExecutionDependentOps(self)(ex)

  type PairPipelineT[F[_], K, V, Ex <: Execution] = DataPipelineT[F, (K, V), Ex]

  /**
    * Operations for [[DataPipelineT]] of tuples
    * (NOT [[MapPipelineT]])
    **/
  implicit class PairPipelineOps[F[_], K, V, Ex <: Execution](
    val self: PairPipelineT[F, K, V, Ex]
  ) extends AnyVal {
    def mapValues[W](
      f: V => W
    )(implicit F: Monad[F]): PairPipelineT[F, K, W, Ex] = self.map {
      case (k, v) => (k, f(v))
    }

    def keys(implicit F: Monad[F], K: ClassTag[K]): DataPipelineT[F, K, Ex] =
      self.map(_._1)

    def values(implicit F: Monad[F], V: ClassTag[V]): DataPipelineT[F, V, Ex] =
      self.map(_._2)

    /**
      * Merges all values [[V]]
      * with the same key [[K]]
      *
      * @param f - reducing function
      * @return - reduced pipeline
      **/
    def reduceByKey(f: (V, V) => V)(implicit ex: Ex,
                                    K: ClassTag[K],
                                    F: Monad[F]): PairPipelineT[F, K, V, Ex] =
      self.groupBy(_._1).mapValues { vs =>
        vs.foldLeft(Option.empty[V]) {
            case (None, (_, v)) => Some(v)
            case (acc, (_, v))  => acc.map(f(_, v))
          }
          .get
      }

    /**
      * Same to [[reduceByKey]] above
      * Merges all values [[V]]
      * having a [[Monoid]] defined
      *
      * @param vMonoid - Monoid for [[V]]
      * @return - reduced pipeline
      **/
    def reduceByKey(implicit vMonoid: Monoid[V],
                    K: ClassTag[K],
                    ex: Ex,
                    F: Monad[F]): PairPipelineT[F, K, V, Ex] =
      reduceByKey((v1, v2) => vMonoid.combine(v1, v2))

    /** @return - [[MapPipelineT]] */
    def toMap(implicit K: ClassTag[K],
              V: ClassTag[V],
              F: Monad[F]): MapPipelineT[F, K, V, Ex] =
      new BaseMapPipelineT[F, K, V, Ex](
        self.asInstanceOf[DataPipelineT[F, (K, V), Ex]],
        F
      )
  }

  /** Implicit conversions */
  implicit def iterable2DataPipeline[A: ClassTag, F[_], Ex <: Execution](
    iterable: Iterable[A]
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    DataPipelineT.liftF[F, A, Ex](iterable.pure[F])

  implicit def array2DataPipeline[A: ClassTag, F[_], Ex <: Execution](
    array: Array[A]
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    DataPipelineT.liftF[F, A, Ex]((array: Iterable[A]).pure[F])

  type Sequential = Execution.Sequential
  type Parallel = Execution.Parallel
}
