package com.honta.trembita.internal


import com.honta.trembita.{BaseLazyList, LazyList}
import com.honta.trembita.parallel.{ParLazyList, ParMapList}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration


trait LazyMap[K, V] extends BaseLazyList[(K, V)] {
  def mapValues[W](f: V => W): LazyMap[K, W]
  def keys: LazyList[K]
  def filterKeys(p: K => Boolean): LazyMap[K, V]
  def values: LazyList[V]
}


protected[trembita] class BaseLazyMap[K, V](source: LazyList[(K, V)])
  extends LazyMap[K, V] {
  override def mapValues[W](f: V => W): LazyMap[K, W] = new BaseLazyMap[K, W](source.mapValues(f))
  override def filterKeys(p: K => Boolean): LazyMap[K, V] = new BaseLazyMap[K, V](source.collect {
    case (k, v) if p(k) => (k, v)
  })
  override def keys: LazyList[K] = source.map(_._1)
  override def values: LazyList[V] = source.map(_._2)
  override def map[B](f: ((K, V)) => B): LazyList[B] = source.groupBy(_._1).map { case (_, vs) => f(vs.head) }
  override def flatMap[B](f: ((K, V)) => Iterable[B]): LazyList[B] = source.groupBy(_._1).flatMap { case (_, vs) => f(vs.head) }
  override def filter(p: ((K, V)) => Boolean): LazyMap[K, V] = new BaseLazyMap[K, V](source.filter(p))

  override def collect[B](pf: PartialFunction[(K, V), B]): LazyList[B] =
    source.groupBy(_._1).flatMap { case (_, vs) => vs.headOption.collect(pf) }

  override def force: Iterable[(K, V)] = {
    val forced: Iterable[(K, V)] = source.force
    forced.groupBy(_._1).map(_._2.head)
  }
  override def par(implicit ec: ExecutionContext): ParLazyList[(K, V)] = new ParMapList[(K, V), (K, V)](identity, source)
  override def iterator: Iterator[(K, V)] = this.force.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: ((K, V)) => Future[B])
                          (implicit ec: ExecutionContext): LazyList[B] =
    new StrictSource[(K, V)](this.force).mapAsync(timeout, parallelism)(f)

}
protected[trembita] class GroupByList[K, V](f: V => K, source: LazyList[V])
  extends BaseLazyList[(K, Iterable[V])] {

  override def map[B](f2: ((K, Iterable[V])) => B): LazyList[B] = LazyList.from(this.force).map(f2)
  override def flatMap[B](f2: ((K, Iterable[V])) => Iterable[B]): LazyList[B] = LazyList.from(this.force).flatMap(f2)
  override def filter(p: ((K, Iterable[V])) => Boolean): LazyList[(K, Iterable[V])] = LazyList.from(this.force).filter(p)
  override def collect[B](pf: PartialFunction[(K, Iterable[V]), B]): LazyList[B] = LazyList.from(this.force).collect(pf)
  override def force: Iterable[(K, Iterable[V])] = source.force.groupBy(f)
  override def par(implicit ec: ExecutionContext): ParLazyList[(K, Iterable[V])] = LazyList.from(this.force).par

  override def iterator: Iterator[(K, Iterable[V])] = this.force.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: ((K, Iterable[V])) => Future[B])
                          (implicit ec: ExecutionContext): LazyList[B] =
    new StrictSource[(K, Iterable[V])](this.force).mapAsync(timeout, parallelism)(f)
}

object LazyMap {
  def apply[K, V](vs: (K, V)*): LazyMap[K, V] = new BaseLazyMap[K, V](LazyList.from(vs.toMap))
  def from[K, V](map: Map[K, V]): LazyMap[K, V] = new BaseLazyMap[K, V](LazyList.from(map))
}
