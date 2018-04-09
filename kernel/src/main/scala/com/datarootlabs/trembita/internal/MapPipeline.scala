package com.datarootlabs.trembita.internal

import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration


/**
  * Special case for [[DataPipeline]]
  * representing a pipeline of tuples
  * with UNIQUE keys
  *
  * @tparam K - key
  * @tparam V - value
  **/
trait MapPipeline[K, V] extends BaseDataPipeline[(K, V)] {
  /**
    * Applies mapping function only for the values
    *
    * @tparam W - resulting value type
    * @param f - transformation function
    * @return - a pipeline with the same key and transformed values
    **/
  def mapValues[W](f: V => W): MapPipeline[K, W]

  /**
    * Returns only those ([[K]], [[V]]) pairs
    * that satisfies given predicate
    *
    * @param p - predicate
    * @return - filtered [[MapPipeline]]
    **/
  def filterKeys(p: K => Boolean): MapPipeline[K, V]

  /** @return - pipeline with keys only */
  def keys: DataPipeline[K]

  /** @return - pipeline with values only */
  def values: DataPipeline[V]
}


/**
  * Sequential implementation of [[MapPipeline]]
  *
  * @tparam K -key
  * @tparam V - value
  * @param source - (K, V) pair pipeline
  **/
protected[trembita] class BaseMapPipeline[K, V](source: DataPipeline[(K, V)]) extends MapPipeline[K, V] {
  def mapValues[W](f: V => W): BaseMapPipeline[K, W] = new BaseMapPipeline[K, W](source.mapValues(f))
  def filterKeys(p: K => Boolean): BaseMapPipeline[K, V] = new BaseMapPipeline[K, V](source.collect {
    case (k, v) if p(k) => (k, v)
  })

  def keys: MappingPipeline[(K, V), K] = new MappingPipeline[(K, V), K](_._1, this)
  def values: MappingPipeline[(K, V), V] = new MappingPipeline[(K, V), V](_._2, this)

  def map[B](f: ((K, V)) => B): MappingPipeline[(K, V), B] = new MappingPipeline[(K, V), B](f, this)
  def flatMap[B](f: ((K, V)) => Iterable[B]): FlatMapPipeline[(K, V), B] = new FlatMapPipeline[(K, V), B](f, this)
  def filter(p: ((K, V)) => Boolean): BaseMapPipeline[K, V] = new BaseMapPipeline[K, V](source.filter(p))
  def collect[B](pf: PartialFunction[(K, V), B]): CollectPipeline[(K, V), B] = new CollectPipeline[(K, V), B](pf, this)

  def eval: Iterable[(K, V)] = {
    val forced: Iterable[(K, V)] = source.eval
    forced.groupBy(_._1).map(_._2.head)
  }

  def par(implicit ec: ExecutionContext): ParDataPipeline[(K, V)] = new ParMapPipeline[(K, V), (K, V)](identity, this)
  def iterator: Iterator[(K, V)] = this.eval.iterator
  def mapAsync[B](timeout: FiniteDuration,
                  parallelism: Int = Runtime.getRuntime.availableProcessors())
                 (f: ((K, V)) => Future[B])
                 (implicit ec: ExecutionContext): DataPipeline[B] =
    new StrictSource[(K, V)](this.eval).mapAsync(timeout, parallelism)(f)

  def :+[B >: (K, V)](elem: B): BaseMapPipeline[K, V] =
    new BaseMapPipeline[K, V]((source :+ elem).asInstanceOf[DataPipeline[(K, V)]])

  def ++[B >: (K, V)](that: DataPipeline[B]): BaseMapPipeline[K, V] =
    new BaseMapPipeline[K, V]((source ++ that).asInstanceOf[DataPipeline[(K, V)]])
}

/**
  * A [[DataPipeline]]
  * been grouped by some criteria
  *
  * @tparam K - grouping criteria type
  * @tparam V - value
  * @param f - grouping function
  **/
protected[trembita] class GroupByPipeline[K, V](f: V => K, source: DataPipeline[V])
  extends BaseDataPipeline[(K, Iterable[V])] {

  def map[B](f2: ((K, Iterable[V])) => B): MappingPipeline[(K, Iterable[V]), B] = new MappingPipeline(f2, this)
  def flatMap[B](f2: ((K, Iterable[V])) => Iterable[B]): FlatMapPipeline[(K, Iterable[V]), B] = new FlatMapPipeline(f2, this)
  def filter(p: ((K, Iterable[V])) => Boolean): DataPipeline[(K, Iterable[V])] =
    new CollectPipeline[(K, Iterable[V]), (K, Iterable[V])](
      { case kvs if p(kvs) ⇒ kvs },
      this
    )

  def collect[B](pf: PartialFunction[(K, Iterable[V]), B]): CollectPipeline[(K, Iterable[V]), B] = new CollectPipeline(pf, this)
  def eval: Iterable[(K, Iterable[V])] = source.eval.groupBy(f)

  def iterator: Iterator[(K, Iterable[V])] = this.eval.iterator

  def mapAsync[B](timeout: FiniteDuration,
                  parallelism: Int = Runtime.getRuntime.availableProcessors())
                 (f: ((K, Iterable[V])) => Future[B])
                 (implicit ec: ExecutionContext): DataPipeline[B] =
    new StrictSource[(K, Iterable[V])](this.eval).mapAsync(timeout, parallelism)(f)

  def par(implicit ec: ExecutionContext): ParDataPipeline[(K, Iterable[V])] = new ParSource[(K, Iterable[V])](this.eval)

  def :+[B >: (K, Iterable[V])](elem: B): DataPipeline[B] = new StrictSource[B](this.eval ++ Some(elem))
  def ++[B >: (K, Iterable[V])](that: DataPipeline[B]): DataPipeline[B] = new StrictSource[B](this.eval ++ that.eval)
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
  def apply[K, V](vs: (K, V)*): BaseMapPipeline[K, V] = from(vs.toMap)

  /**
    * Creates [[MapPipeline]] from given map
    *
    * @tparam K - key
    * @tparam V - value
    * @param map - map
    * @return - a MapPipeline
    **/
  def from[K, V](map: Map[K, V]): BaseMapPipeline[K, V] = new BaseMapPipeline[K, V](DataPipeline.from(map))
}
