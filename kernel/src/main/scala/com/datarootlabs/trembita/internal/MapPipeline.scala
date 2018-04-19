package com.datarootlabs.trembita.internal

import com.datarootlabs.trembita._
import cats._
import cats.implicits._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Try

/**
  * Special case for [[DataPipeline]]
  * representing a pipeline of tuples
  * with UNIQUE keys
  *
  * @tparam K - key
  * @tparam V - value
  **/
trait MapPipeline[K, V, F[_], Ex <: Execution] extends DataPipeline[(K, V), F, Finiteness.Finite, Ex] {
  /**
    * Applies mapping function only for the values
    *
    * @tparam W - resulting value type
    * @param f - transformation function
    * @return - a pipeline with the same key and transformed values
    **/
  def mapValues[W](f: V => W): MapPipeline[K, W, F, Ex]

  /**
    * Returns only those ([[K]], [[V]]) pairs
    * that satisfies given predicate
    *
    * @param p - predicate
    * @return - filtered [[MapPipeline]]
    **/
  def filterKeys(p: K => Boolean): MapPipeline[K, V, F, Ex]

  /** @return - pipeline with keys only */
  def keys: DataPipeline[K, F, Finiteness.Finite, Ex]

  /** @return - pipeline with values only */
  def values: DataPipeline[V, F, Finiteness.Finite, Ex]
}


/**
  * Sequential implementation of [[MapPipeline]]
  *
  * @tparam K -key
  * @tparam V - value
  * @param source - (K, V) pair pipeline
  **/
protected[trembita]
class BaseMapPipeline[K, V, F[_], Ex <: Execution]
(source: DataPipeline[(K, V), F, Finiteness.Finite, Ex], ex: Ex)
(implicit ME: MonadError[F, Throwable]
) extends SeqSource[(K, V), F, Finiteness.Finite, Ex]
  with MapPipeline[K, V, F, Ex] {

  def mapValues[W](f: V => W): MapPipeline[K, W, F, Ex] = new BaseMapPipeline[K, W, F, Ex](source.mapValues(f), ex)

  def filterKeys(p: K => Boolean): MapPipeline[K, V, F, Ex] = new BaseMapPipeline[K, V, F, Ex](source.collect {
    case (k, v) if p(k) => (k, v)
  }, ex)

  def keys: DataPipeline[K, F, Finiteness.Finite, Ex] =
    new MappingPipeline[(K, V), K, F, Finiteness.Finite, Ex](_._1, this)

  def values: DataPipeline[V, F, Finiteness.Finite, Ex] =
    new MappingPipeline[(K, V), V, F, Finiteness.Finite, Ex](_._2, this)

  protected[trembita]
  def evalFunc[B >: (K, V)](implicit ev: <:<[Finiteness.Finite, Finiteness.Finite], Ex: Ex): F[Ex.Repr[B]] =
    source.evalFunc[(K, V)](ev, Ex).map(vs => Ex.fromVector(Ex.toVector(vs).toMap.toVector))

  protected[trembita]
  def bindFunc[B >: (K, V)](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit] = evalFunc[(K, V)]($conforms, ex).map { repr =>
    ex.Traverse.traverse[F, B, Unit](repr.asInstanceOf[ex.Repr[B]])(b => f(Right(b)))
  }

  def handleError[B >: (K, V)](f: Throwable ⇒ B): DataPipeline[B, F, Finiteness.Finite, Ex] =
    new BaseMapPipeline[K, V, F, Ex](source.handleError(f).asInstanceOf[DataPipeline[(K, V), F, Finiteness.Finite, Ex]], ex)

  def handleErrorWith[B >: (K, V)](f: Throwable ⇒ DataPipeline[B, F, Finiteness.Finite, Ex]): DataPipeline[B, F, Finiteness.Finite, Ex] =
    new BaseMapPipeline[K, V, F, Ex](source.handleErrorWith(f).asInstanceOf[DataPipeline[(K, V), F, Finiteness.Finite, Ex]], ex)
}

/**
  * A [[DataPipeline]]
  * been grouped by some criteria
  *
  * @tparam K - grouping criteria type
  * @tparam V - value
  * @param f - grouping function
  **/
protected[trembita]
class GroupByPipeline[K, V, F[_], Ex <: Execution]
(f: V => K, source: DataPipeline[V, F, Finiteness.Finite, Ex], ex: Ex)
(implicit ME: MonadError[F, Throwable]
) extends SeqSource[(K, Iterable[V]), F, Finiteness.Finite, Ex] {

  def handleError[B >: (K, Iterable[V])](f: Throwable ⇒ B): DataPipeline[B, F, Finiteness.Finite, Ex] =
    this

  def handleErrorWith[B >: (K, Iterable[V])](f: Throwable ⇒ DataPipeline[B, F, Finiteness.Finite, Ex]): DataPipeline[B, F, Finiteness.Finite, Ex] =
    this

  protected[trembita]
  def evalFunc[B >: (K, Iterable[V])](implicit ev: <:<[Finiteness.Finite, Finiteness.Finite], Ex: Ex): F[Ex.Repr[B]] =
    source.evalFunc[V](ev, Ex).map(vs => Ex.fromVector(Ex.groupBy(vs)(f).toVector).asInstanceOf[Ex.Repr[B]])

  protected[trembita] def bindFunc[B >: (K, Iterable[V])](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit] =
    evalFunc[B]($conforms, ex).map { vs =>
      ex.Traverse.traverse[F, B, Unit](vs.asInstanceOf[ex.Repr[B]])(b => f(Right(b)))
    }
}

object MapPipeline {
  /**
    * Creates [[MapPipeline]] from given pairs
    *
    * @tparam K - key
    * @tparam V - value
    * @param vs - pairs
    * @return - a MapPipeline
    **/
  def apply[K, V](vs: (K, V)*): BaseMapPipeline[K, V, Try, Execution.Sequential] = from(vs.toMap)

  def applyF[K, V, F[_]](vs: (K, V)*)(implicit F: MonadError[F, Throwable]): BaseMapPipeline[K, V, F, Execution.Sequential] =
    new BaseMapPipeline[K, V, F, Execution.Sequential](DataPipeline.applyF(vs: _*), Execution.Sequential)

  /**
    * Creates [[MapPipeline]] from given map
    *
    * @tparam K - key
    * @tparam V - value
    * @param map - map
    * @return - a MapPipeline
    **/
  def from[K, V](map: Map[K, V]): BaseMapPipeline[K, V, Try, Execution.Sequential] =
    new BaseMapPipeline[K, V, Try, Execution.Sequential](DataPipeline.from(map), Execution.Sequential)

  def fromEffect[K, V, F[_], Ex <: Execution](mapF: F[Map[K, V]])(implicit F: MonadError[F, Throwable], Ex: Ex): BaseMapPipeline[K, V, F, Ex] =
    new BaseMapPipeline[K, V, F, Ex](DataPipeline.fromEffect[(K, V), F, Finiteness.Finite, Ex](mapF.asInstanceOf[F[Iterable[(K, V)]]]), Ex)
}
