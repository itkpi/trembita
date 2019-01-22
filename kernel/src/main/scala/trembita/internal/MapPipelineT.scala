package trembita.internal

import trembita._
import cats._
import cats.implicits._
import trembita.operations.{CanGroupBy, CanGroupByOrdered}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.Try

/**
  * Special case for [[DataPipelineT]]
  * representing a pipeline of tuples
  * with UNIQUE keys
  *
  * @tparam K - key
  * @tparam V - value
  **/
trait MapPipelineT[F[_], K, V, Ex <: Environment]
    extends DataPipelineT[F, (K, V), Ex] {

  /**
    * Applies mapping function only for the values
    *
    * @tparam W - resulting value type
    * @param f - transformation function
    * @return - a pipeline with the same key and transformed values
    **/
  def mapValues[W: ClassTag](f: V => W)(implicit F: Monad[F]): MapPipelineT[F, K, W, Ex]

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
protected[trembita] class BaseMapPipelineT[F[_], K, V, Ex <: Environment](
  source: DataPipelineT[F, (K, V), Ex],
  F: Monad[F]
)(implicit K: ClassTag[K], V: ClassTag[V]) extends SeqSource[F, (K, V), Ex](F)
    with MapPipelineT[F, K, V, Ex] {

  def mapValues[W: ClassTag](f: V => W)(implicit F: Monad[F]): MapPipelineT[F, K, W, Ex@uncheckedVariance] =
    new BaseMapPipelineT[F, K, W, Ex](source.mapImpl{case (k, v) => k -> f(v)}, F)

  def filterKeys(
    p: K => Boolean
  )(implicit F: Monad[F]): MapPipelineT[F, K, V, Ex] =
    new BaseMapPipelineT[F, K, V, Ex](source.collectImpl {
      case (k, v) if p(k) => (k, v)
    }, F)

  def keys(implicit F: Monad[F]): DataPipelineT[F, K, Ex] =
    new MappingPipelineT[F, (K, V), K, Ex](_._1, this)(F)

  def values(implicit F: Monad[F]): DataPipelineT[F, V, Ex] =
    new MappingPipelineT[F, (K, V), V, Ex](_._2, this)(F)

  protected[trembita] def evalFunc[B >: (K, V)](Ex: Ex)(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(
      source
        .evalFunc[(K, V)](Ex)
    )(vs => Ex.distinctKeys(vs).asInstanceOf[Ex.Repr[B]])

  override def handleErrorImpl[B >: (K, V): ClassTag](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new BaseMapPipelineT[F, K, V, Ex](
      source
        .handleErrorImpl(f)
        .asInstanceOf[DataPipelineT[F, (K, V), Ex]],
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
object GroupByPipelineT {
  def make[F[_], K, V, Ex <: Environment](
                                           f: V => K,
                                           source: DataPipelineT[F, V, Ex],
                                           F: Monad[F],
                                           canGroupBy: CanGroupBy[Ex#Repr]
                                         )(
    implicit K: ClassTag[K], V: ClassTag[V]
  ): DataPipelineT[F, (K, Iterable[V]), Ex]   =
 new SeqSource[F, (K, Iterable[V]), Ex](F) {
  protected[trembita] def evalFunc[B >: (K, Iterable[V])](Ex: Ex)(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(
      source
        .evalFunc[V](Ex)
    )(
      vs =>
        canGroupBy.groupBy(vs.asInstanceOf[Ex#Repr[V]])(f).asInstanceOf[Ex.Repr[B]]
    )
  }
}

/**
  * A [[DataPipelineT]]
  * been grouped by some criteria
  *
  * @tparam K - grouping criteria type
  * @tparam V - value
  * @param f - grouping function
  **/
object GroupByOrderedPipelineT {
  def make[F[_], K, V, Ex <: Environment](
                                           f: V => K,
                                           source: DataPipelineT[F, V, Ex],
                                           F: Monad[F],
                                           canGroupBy: CanGroupByOrdered[Ex#Repr]
                                         )(
                                           implicit K: ClassTag[K], V: ClassTag[V],
                                           ordering: Ordering[K]
                                         ): DataPipelineT[F, (K, Iterable[V]), Ex]   =
    new SeqSource[F, (K, Iterable[V]), Ex](F) {
      protected[trembita] def evalFunc[B >: (K, Iterable[V])](Ex: Ex)(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
        F.map(
          source
            .evalFunc[V](Ex)
        )(
          vs =>
            canGroupBy.groupBy(vs.asInstanceOf[Ex#Repr[V]])(f).asInstanceOf[Ex.Repr[B]]
        )
    }
}
