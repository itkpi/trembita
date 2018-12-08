package com.github.trembita.internal

import com.github.trembita._
import cats._
import cats.implicits._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Try

/**
  * Special case for [[DataPipelineT]]
  * representing a pipeline of tuples
  * with UNIQUE keys
  *
  * @tparam K - key
  * @tparam V - value
  **/
trait MapPipelineT[F[_], K, V, Ex <: Execution]
    extends DataPipelineT[F, (K, V), Ex] {

  /**
    * Applies mapping function only for the values
    *
    * @tparam W - resulting value type
    * @param f - transformation function
    * @return - a pipeline with the same key and transformed values
    **/
  def mapValues[W](f: V => W)(implicit F: Monad[F]): MapPipelineT[F, K, W, Ex]

  /**
    * Returns only those ([[K]], [[V]]) pairs
    * that satisfies given predicate
    *
    * @param p - predicate
    * @return - filtered [[MapPipelineT]]
    **/
  def filterKeys(p: K => Boolean)(
    implicit F: Monad[F]
  ): MapPipelineT[F, K, V, Ex]

  /** @return - pipeline with keys only */
  def keys(implicit F: Monad[F]): DataPipelineT[F, K, Ex]

  /** @return - pipeline with values only */
  def values(implicit F: Monad[F]): DataPipelineT[F, V, Ex]
}

/**
  * Sequential implementation of [[MapPipelineT]]
  *
  * @tparam K -key
  * @tparam V - value
  * @param source - (K, V) pair pipeline
  **/
protected[trembita] class BaseMapPipelineT[F[_], K, V, Ex <: Execution](
  source: DataPipelineT[F, (K, V), Ex],
  ex: Ex,
  F: Monad[F]
) extends SeqSource[F, (K, V), Ex](F)
    with MapPipelineT[F, K, V, Ex] {

  def mapValues[W](f: V => W)(implicit F: Monad[F]): MapPipelineT[F, K, W, Ex] =
    new BaseMapPipelineT[F, K, W, Ex](source.mapValues(f), ex, F)

  def filterKeys(
    p: K => Boolean
  )(implicit F: Monad[F]): MapPipelineT[F, K, V, Ex] =
    new BaseMapPipelineT[F, K, V, Ex](source.collect {
      case (k, v) if p(k) => (k, v)
    }, ex, F)

  def keys(implicit F: Monad[F]): DataPipelineT[F, K, Ex] =
    new MappingPipelineT[F, (K, V), K, Ex](_._1, this)(F)

  def values(implicit F: Monad[F]): DataPipelineT[F, V, Ex] =
    new MappingPipelineT[F, (K, V), V, Ex](_._2, this)(F)

  protected[trembita] def evalFunc[B >: (K, V)](Ex: Ex): F[Ex.Repr[B]] =
    F.map(
      source
        .evalFunc[(K, V)](Ex)
    )(vs => Ex.fromVector(Ex.toVector(vs).toMap.toVector))

  def handleError[B >: (K, V)](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new BaseMapPipelineT[F, K, V, Ex](
      source
        .handleError(f)
        .asInstanceOf[DataPipelineT[F, (K, V), Ex]],
      ex,
      F
    )

  def handleErrorWith[B >: (K, V)](
    f: Throwable => DataPipelineT[F, B, Ex]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new BaseMapPipelineT[F, K, V, Ex](
      source
        .handleErrorWith(f)
        .asInstanceOf[DataPipelineT[F, (K, V), Ex]],
      ex,
      F
    )
}

/**
  * A [[DataPipelineT]]
  * been grouped by some criteria
  *
  * @tparam K - grouping criteria type
  * @tparam V - value
  * @param f - grouping function
  **/
protected[trembita] class GroupByPipelineT[F[_], K, V, Ex <: Execution](
  f: V => K,
  source: DataPipelineT[F, V, Ex],
  ex: Ex,
  F: Monad[F]
) extends SeqSource[F, (K, Vector[V]), Ex](F) {

  def handleError[B >: (K, Vector[V])](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    this

  def handleErrorWith[B >: (K, Vector[V])](
    f: Throwable => DataPipelineT[F, B, Ex]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    this

  protected[trembita] def evalFunc[B >: (K, Vector[V])](Ex: Ex): F[Ex.Repr[B]] =
    F.map(
      source
        .evalFunc[V](ex)
    )(
      vs =>
        Ex.fromVector(
          Ex.groupBy(Ex.fromVector(ex.toVector(vs.asInstanceOf[ex.Repr[V]])))(f)
            .mapValues(Ex.toVector)
            .toVector
      )
    )
}

object MapPipelineT {
//
//  /**
//    * Creates [[MapPipelineT]] from given pairs
//    *
//    * @tparam K - key
//    * @tparam V - value
//    * @param vs - pairs
//    * @return - a MapPipeline
//    **/
//  def apply[K, V](
//    vs: (K, V)*
//  ): BaseMapPipelineT[K, V, Try, Execution.Sequential] = from(vs.toMap)
//
//  def applyF[K, V, F[_]](vs: (K, V)*)(
//    implicit F: MonadError[F, Throwable]
//  ): BaseMapPipelineT[K, V, F, Execution.Sequential] =
//    new BaseMapPipelineT[K, V, F, Execution.Sequential](
//      DataPipelineT.applyF(vs: _*),
//      Execution.Sequential
//    )
//
//  /**
//    * Creates [[MapPipelineT]] from given map
//    *
//    * @tparam K - key
//    * @tparam V - value
//    * @param map - map
//    * @return - a MapPipeline
//    **/
//  def from[K, V](
//    map: Map[K, V]
//  ): BaseMapPipelineT[K, V, Try, Execution.Sequential] =
//    new BaseMapPipelineT[K, V, Try, Execution.Sequential](
//      DataPipelineT.from(map),
//      Execution.Sequential
//    )
//
//  def fromEffect[K, V, F[_], Ex <: Execution](mapF: F[Map[K, V]])(
//    implicit F: MonadError[F, Throwable],
//    Ex: Ex
//  ): BaseMapPipelineT[F, K, V, Ex] =
//    new BaseMapPipelineT[F, K, V, Ex](
//      DataPipelineT.fromEffect[(K, V), F, Ex](
//        mapF.asInstanceOf[F[Iterable[(K, V)]]]
//      ),
//      Ex
//    )
}
