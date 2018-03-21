package com.datarootlabs.trembita.internal

import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration


trait MapPipeline[K, V] extends BaseDataPipeline[(K, V)] {
  def mapValues[W](f: V => W): MapPipeline[K, W]
  def keys: DataPipeline[K]
  def filterKeys(p: K => Boolean): MapPipeline[K, V]
  def values: DataPipeline[V]
}


protected[trembita] class BaseMapPipeline[K, V](source: DataPipeline[(K, V)])
  extends MapPipeline[K, V] {
  override def mapValues[W](f: V => W): MapPipeline[K, W] = new BaseMapPipeline[K, W](source.mapValues(f))
  override def filterKeys(p: K => Boolean): MapPipeline[K, V] = new BaseMapPipeline[K, V](source.collect {
    case (k, v) if p(k) => (k, v)
  })
  override def keys: DataPipeline[K] = source.map(_._1)
  override def values: DataPipeline[V] = source.map(_._2)
  override def map[B](f: ((K, V)) => B): DataPipeline[B] = source.groupBy(_._1).map { case (_, vs) => f(vs.head) }
  override def flatMap[B](f: ((K, V)) => Iterable[B]): DataPipeline[B] = source.groupBy(_._1).flatMap { case (_, vs) => f(vs.head) }
  override def filter(p: ((K, V)) => Boolean): MapPipeline[K, V] = new BaseMapPipeline[K, V](source.filter(p))

  override def collect[B](pf: PartialFunction[(K, V), B]): DataPipeline[B] =
    source.groupBy(_._1).flatMap { case (_, vs) => vs.headOption.collect(pf) }

  override def force: Iterable[(K, V)] = {
    val forced: Iterable[(K, V)] = source.force
    forced.groupBy(_._1).map(_._2.head)
  }
  override def par(implicit ec: ExecutionContext): ParDataPipeline[(K, V)] = new ParMapPipeline[(K, V), (K, V)](identity, source)
  override def iterator: Iterator[(K, V)] = this.force.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: ((K, V)) => Future[B])
                          (implicit ec: ExecutionContext): DataPipeline[B] =
    new StrictSource[(K, V)](this.force).mapAsync(timeout, parallelism)(f)

}
protected[trembita] class GroupByList[K, V](f: V => K, source: DataPipeline[V])
  extends BaseDataPipeline[(K, Iterable[V])] {

  override def map[B](f2: ((K, Iterable[V])) => B): DataPipeline[B] = DataPipeline.from(this.force).map(f2)
  override def flatMap[B](f2: ((K, Iterable[V])) => Iterable[B]): DataPipeline[B] = DataPipeline.from(this.force).flatMap(f2)
  override def filter(p: ((K, Iterable[V])) => Boolean): DataPipeline[(K, Iterable[V])] = DataPipeline.from(this.force).filter(p)
  override def collect[B](pf: PartialFunction[(K, Iterable[V]), B]): DataPipeline[B] = DataPipeline.from(this.force).collect(pf)
  override def force: Iterable[(K, Iterable[V])] = source.force.groupBy(f)
  override def par(implicit ec: ExecutionContext): ParDataPipeline[(K, Iterable[V])] = DataPipeline.from(this.force).par

  override def iterator: Iterator[(K, Iterable[V])] = this.force.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: ((K, Iterable[V])) => Future[B])
                          (implicit ec: ExecutionContext): DataPipeline[B] =
    new StrictSource[(K, Iterable[V])](this.force).mapAsync(timeout, parallelism)(f)
}

object MapPipeline {
  def apply[K, V](vs: (K, V)*): MapPipeline[K, V] = new BaseMapPipeline[K, V](DataPipeline.from(vs.toMap))
  def from[K, V](map: Map[K, V]): MapPipeline[K, V] = new BaseMapPipeline[K, V](DataPipeline.from(map))
}
