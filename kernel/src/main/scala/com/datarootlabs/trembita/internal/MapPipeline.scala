//package com.datarootlabs.trembita.internal
//
//import com.datarootlabs.trembita._
//import com.datarootlabs.trembita.parallel._
//import scala.concurrent.{ExecutionContext, Future}
//import scala.concurrent.duration.FiniteDuration
//
//
///**
//  * Special case for [[DataPipeline]]
//  * representing a pipeline of tuples
//  * with UNIQUE keys
//  *
//  * @tparam K - key
//  * @tparam V - value
//  **/
//trait MapPipeline[K, V, F[_], T <: PipelineType] extends DataPipeline[(K, V), F, T] {
//  /**
//    * Applies mapping function only for the values
//    *
//    * @tparam W - resulting value type
//    * @param f - transformation function
//    * @return - a pipeline with the same key and transformed values
//    **/
//  def mapValues[W](f: V => W): MapPipeline[K, W, F, T]
//
//  /**
//    * Returns only those ([[K]], [[V]]) pairs
//    * that satisfies given predicate
//    *
//    * @param p - predicate
//    * @return - filtered [[MapPipeline]]
//    **/
//  def filterKeys(p: K => Boolean): MapPipeline[K, V, F, T]
//
//  /** @return - pipeline with keys only */
//  def keys: DataPipeline[K, F, T]
//
//  /** @return - pipeline with values only */
//  def values: DataPipeline[V, F, T]
//}
//
//
///**
//  * Sequential implementation of [[MapPipeline]]
//  *
//  * @tparam K -key
//  * @tparam V - value
//  * @param source - (K, V) pair pipeline
//  **/
//protected[trembita] class BaseMapPipeline[K, V, F[_], T <: PipelineType]
//(source: DataPipeline[(K, V), F, T]
//) extends MapPipeline[K, V, F, T] {
//  def mapValues[W](f: V => W): BaseMapPipeline[K, W, F, T] = new BaseMapPipeline[K, W, F, T](source.mapValues(f))
//  def filterKeys(p: K => Boolean): BaseMapPipeline[K, V, F, T] = new BaseMapPipeline[K, V, F, T](source.collect {
//    case (k, v) if p(k) => (k, v)
//  })
//
//  def keys: MappingPipeline[(K, V), K, F, T] = new MappingPipeline[(K, V), K, F, T](_._1, this)
//  def values: MappingPipeline[(K, V), V, F, T] = new MappingPipeline[(K, V), V, F, T](_._2, this)
//
//  def map[B](f: ((K, V)) => B): MappingPipeline[(K, V), B, F, T] = new MappingPipeline[(K, V), B, F, T](f, this)
//  def flatMap[B](f: ((K, V)) => Iterable[B]): FlatMapPipeline[(K, V), B, F, T] = new FlatMapPipeline[(K, V), B, F, T](f, this)
//  def filter(p: ((K, V)) => Boolean): BaseMapPipeline[K, V, F, T] = new BaseMapPipeline[K, V, F, T](source.filter(p))
//  def collect[B](pf: PartialFunction[(K, V), B]): CollectPipeline[(K, V), B, F, T] = new CollectPipeline[(K, V), B, F, T](pf, this)
//}
//
///**
//  * A [[DataPipeline]]
//  * been grouped by some criteria
//  *
//  * @tparam K - grouping criteria type
//  * @tparam V - value
//  * @param f - grouping function
//  **/
//protected[trembita] class GroupByPipeline[K, V, F[_], T <: PipelineType](f: V => K, source: DataPipeline[V, F, T])
//  extends DataPipeline[(K, Iterable[V]), F, T] {
//
//  def map[B](f2: ((K, Iterable[V])) => B): MappingPipeline[(K, Iterable[V]), B, F, T] = new MappingPipeline(f2, this)
//  def flatMap[B](f2: ((K, Iterable[V])) => Iterable[B]): FlatMapPipeline[(K, Iterable[V]), B, F, T] =
//    new FlatMapPipeline(f2, this)
//
//  def filter(p: ((K, Iterable[V])) => Boolean): DataPipeline[(K, Iterable[V]), F, T] =
//    new CollectPipeline[(K, Iterable[V]), (K, Iterable[V]), F, T](
//      { case kvs if p(kvs) ⇒ kvs },
//      this
//    )
//
//  def collect[B](pf: PartialFunction[(K, Iterable[V]), B]): CollectPipeline[(K, Iterable[V]), B, F, T] =
//    new CollectPipeline(pf, this)
//}
//
//object MapPipeline {
//  /**
//    * Creates [[MapPipeline]] from given pairs
//    *
//    * @tparam K - key
//    * @tparam V - value
//    * @param vs - pairs
//    * @return - a MapPipeline
//    **/
//  def apply[K, V](vs: (K, V)*): BaseMapPipeline[K, V] = from(vs.toMap)
//
//  /**
//    * Creates [[MapPipeline]] from given map
//    *
//    * @tparam K - key
//    * @tparam V - value
//    * @param map - map
//    * @return - a MapPipeline
//    **/
//  def from[K, V](map: Map[K, V]): BaseMapPipeline[K, V] = new BaseMapPipeline[K, V](DataPipeline.from(map))
//}
