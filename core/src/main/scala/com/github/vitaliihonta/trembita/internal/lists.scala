package com.github.vitaliihonta.trembita.internal


import com.github.vitaliihonta.trembita._
import com.github.vitaliihonta.trembita.parallel._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag


protected[trembita]
class MapList[+A, B](f: A => B, source: LazyList[A]) extends BaseLazyList[B] {
  override def map[C](f2: B => C): LazyList[C] = new MapList[A, C](f2.compose(f), source)
  override def flatMap[C](f2: B => Iterable[C]): LazyList[C] = {
    new FlatMapList[A, C](f2.compose(f), source)
  }
  override def filter(p: B => Boolean): LazyList[B] = new FlatMapList[A, B](
    a => Some(f(a)).filter(p),
    source
  )

  override def collect[C](pf: PartialFunction[B, C]): LazyList[C] = new FlatMapList[A, C](
    a => Some(f(a)).collect(pf),
    source
  )

  override def force: Iterable[B] = source.force.map(f)
  override def par(implicit ec: ExecutionContext): ParLazyList[B] = new ParMapList[A, B](f, source)
  override def iterator: Iterator[B] = source.iterator.map(f)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f2: B => Future[C])(implicit ec: ExecutionContext): LazyList[C] =
    new MapList[A, C]({ a =>
      val future = f2(f(a))
      Await.result(future, timeout)
    }, source)
}

protected[trembita]
class FlatMapList[+A, B](f: A => Iterable[B], source: LazyList[A]) extends BaseLazyList[B] {
  override def map[C](f2: B => C): LazyList[C] = {
    new FlatMapList[A, C](
      a => f(a).map(f2),
      source
    )
  }

  override def flatMap[C](f2: B => Iterable[C]): LazyList[C] = {
    new FlatMapList[A, C](
      a => f(a).flatMap(f2),
      source
    )
  }

  override def filter(p: B => Boolean): LazyList[B] = new FlatMapList[A, B](
    a => f(a).filter(p),
    source
  )

  override def collect[C](pf: PartialFunction[B, C]): LazyList[C] = new FlatMapList[A, C](
    a => f(a).collect(pf),
    source
  )

  override def force: Iterable[B] = source.force.flatMap(f)
  override def par(implicit ec: ExecutionContext): ParLazyList[B] = new ParFlatMapList[A, B](f, source)
  override def iterator: Iterator[B] = source.force.iterator.flatMap(f)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f2: B => Future[C])(implicit ec: ExecutionContext): LazyList[C] = {
    new FlatMapList[A, C]({ a =>
      val future = Future.sequence(f(a).map(f2))
      Await.result(future, timeout)
    }, source)
  }

}

protected[trembita]
class CollectList[+A, B](pf: PartialFunction[A, B], source: LazyList[A]) extends BaseLazyList[B] {
  override def map[C](f: B => C): LazyList[C] = new CollectList[A, C](
    {
      case a if pf isDefinedAt a => f(pf(a))
    },
    source
  )
  override def flatMap[BB](f: B => Iterable[BB]): LazyList[BB] = {
    new FlatMapList[A, BB](
      a =>
        if (pf isDefinedAt a) f(pf(a))
        else Iterable.empty,
      source
    )
  }

  override def filter(p: B => Boolean): LazyList[B] = new CollectList[A, B](
    pf.andThen {
      case b if p(b) => b
    },
    source
  )
  override def collect[BB](pf2: PartialFunction[B, BB]): LazyList[BB] = new CollectList[A, BB](
    pf.andThen(pf2),
    source
  )

  override def force: Iterable[B] = source.force.collect(pf)
  override def par(implicit ec: ExecutionContext): ParLazyList[B] = new ParCollectList[A, B](pf, source)
  override def iterator: Iterator[B] = source.force.iterator.collect(pf)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: B => Future[C])
                          (implicit ec: ExecutionContext): LazyList[C] = {
    new CollectList[A, C]({
      case a if pf.isDefinedAt(a) =>
        val future = f(pf(a))
        Await.result(future, timeout)
    }, source)
  }
}


protected[trembita]
trait SeqSource[+A] extends BaseLazyList[A] {
  override def map[B](f: A => B): LazyList[B] = new MapList[A, B](f, this)
  override def flatMap[B](f: A => Iterable[B]): LazyList[B] = new FlatMapList[A, B](f, this)
  override def filter(p: A => Boolean): LazyList[A] = new CollectList[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): LazyList[B] = new CollectList[A, B](pf, this)
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: A => Future[B])
                          (implicit ec: ExecutionContext): LazyList[B] = {
    new MapList[A, B]({ a =>
      val future = f(a)
      Await.result(future, timeout)
    }, this)
  }
}

protected[trembita]
class StrictSource[+A](iterable: => Iterable[A]) extends SeqSource[A] {
  override def force: Iterable[A] = iterable
  override def par(implicit ec: ExecutionContext): ParLazyList[A] = new ParSource[A](iterable)
  override def take(n: Int): Iterable[A] = iterable.take(n).toVector
  override def drop(n: Int): Iterable[A] = iterable.drop(n).toVector
  override def headOption: Option[A] = iterable.headOption
  override def iterator: Iterator[A] = iterable.iterator
}

protected[trembita]
class CachedSource[+A](iterable: Iterable[A]) extends SeqSource[A] {
  override def force: Iterable[A] = iterable
  override def par(implicit ec: ExecutionContext): ParLazyList[A] = new ParSource[A](iterable)
  override def take(n: Int): Iterable[A] = iterable.take(n).toVector
  override def drop(n: Int): Iterable[A] = iterable.drop(n).toVector
  override def headOption: Option[A] = iterable.headOption
  override def iterator: Iterator[A] = iterable.iterator
}
protected[trembita]
class IteratorSource[+A](xiterator: Iterator[A]) extends SeqSource[A] {
  override def force: Iterable[A] = xiterator.toIterable
  override def par(implicit ec: ExecutionContext): ParLazyList[A] = new ParSource[A](xiterator.toIterable)
  override def take(n: Int): Iterable[A] = force.take(n).toVector
  override def drop(n: Int): Iterable[A] = force.drop(n).toVector
  override def headOption: Option[A] = force.headOption
  override def iterator: Iterator[A] = xiterator
}

protected[trembita]
class SortedSource[+A: Ordering : ClassTag](source: LazyList[A]) extends SeqSource[A] {
  override def sorted[B >: A : Ordering : ClassTag]: LazyList[B] = this
  override def force: Iterable[A] = {
    val forced: Iterable[A] = source.force
    forced.toVector.sorted
  }
  override def par(implicit ec: ExecutionContext): ParLazyList[A] = new ParSortedSource[A](this)
  override def iterator: Iterator[A] = this.force.iterator
}
