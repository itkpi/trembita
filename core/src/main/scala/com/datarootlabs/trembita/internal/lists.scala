package com.datarootlabs.trembita.internal


import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag


protected[trembita]
class MappingPipeline[+A, B](f: A => B, source: DataPipeline[A]) extends BaseDataPipeline[B] {
  override def map[C](f2: B => C): DataPipeline[C] = new MappingPipeline[A, C](f2.compose(f), source)
  override def flatMap[C](f2: B => Iterable[C]): DataPipeline[C] = {
    new FlatMapPipeline[A, C](f2.compose(f), source)
  }
  override def filter(p: B => Boolean): DataPipeline[B] = new FlatMapPipeline[A, B](
    a => Some(f(a)).filter(p),
    source
  )

  override def collect[C](pf: PartialFunction[B, C]): DataPipeline[C] = new FlatMapPipeline[A, C](
    a => Some(f(a)).collect(pf),
    source
  )

  override def force: Iterable[B] = source.force.map(f)
  override def par(implicit ec: ExecutionContext): ParDataPipeline[B] = new ParMapPipeline[A, B](f, source)
  override def iterator: Iterator[B] = source.iterator.map(f)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f2: B => Future[C])(implicit ec: ExecutionContext): DataPipeline[C] =
    new MappingPipeline[A, C]({ a =>
      val future = f2(f(a))
      Await.result(future, timeout)
    }, source)

  override def :+[BB >: B](elem: BB): DataPipeline[BB] = new StrictSource(this.force ++ Some(elem))
  override def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource[BB](this.force ++ that.force)
}

protected[trembita]
class FlatMapPipeline[+A, B](f: A => Iterable[B], source: DataPipeline[A]) extends BaseDataPipeline[B] {
  override def map[C](f2: B => C): DataPipeline[C] = {
    new FlatMapPipeline[A, C](
      a => f(a).map(f2),
      source
    )
  }

  override def flatMap[C](f2: B => Iterable[C]): DataPipeline[C] = {
    new FlatMapPipeline[A, C](
      a => f(a).flatMap(f2),
      source
    )
  }

  override def filter(p: B => Boolean): DataPipeline[B] = new FlatMapPipeline[A, B](
    a => f(a).filter(p),
    source
  )

  override def collect[C](pf: PartialFunction[B, C]): DataPipeline[C] = new FlatMapPipeline[A, C](
    a => f(a).collect(pf),
    source
  )

  override def force: Iterable[B] = source.force.flatMap(f)
  override def par(implicit ec: ExecutionContext): ParDataPipeline[B] = new ParFlatMapPipeline[A, B](f, source)
  override def iterator: Iterator[B] = source.force.iterator.flatMap(f)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f2: B => Future[C])(implicit ec: ExecutionContext): DataPipeline[C] = {
    new FlatMapPipeline[A, C]({ a =>
      val future = Future.sequence(f(a).map(f2))
      Await.result(future, timeout)
    }, source)
  }

  override def :+[BB >: B](elem: BB): DataPipeline[BB] = new StrictSource(this.force ++ List(elem))
  override def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(this.force ++ that.force)
}

protected[trembita]
class CollectPipeline[+A, B](pf: PartialFunction[A, B], source: DataPipeline[A]) extends BaseDataPipeline[B] {
  override def map[C](f: B => C): DataPipeline[C] = new CollectPipeline[A, C](
    {
      case a if pf isDefinedAt a => f(pf(a))
    },
    source
  )
  override def flatMap[BB](f: B => Iterable[BB]): DataPipeline[BB] = {
    new FlatMapPipeline[A, BB](
      a =>
        if (pf isDefinedAt a) f(pf(a))
        else Iterable.empty,
      source
    )
  }

  override def filter(p: B => Boolean): DataPipeline[B] = new CollectPipeline[A, B](
    pf.andThen {
      case b if p(b) => b
    },
    source
  )
  override def collect[BB](pf2: PartialFunction[B, BB]): DataPipeline[BB] = new CollectPipeline[A, BB](
    pf.andThen(pf2),
    source
  )

  override def force: Iterable[B] = source.force.collect(pf)
  override def par(implicit ec: ExecutionContext): ParDataPipeline[B] = new ParCollectPipeline[A, B](pf, source)
  override def iterator: Iterator[B] = source.force.iterator.collect(pf)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: B => Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = {
    new CollectPipeline[A, C]({
      case a if pf.isDefinedAt(a) =>
        val future = f(pf(a))
        Await.result(future, timeout)
    }, source)
  }

  override def :+[BB >: B](elem: BB): DataPipeline[BB] = new StrictSource(this.force ++ Some(elem))
  override def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource[BB](this.force ++ that.force)
}


protected[trembita]
trait SeqSource[+A] extends BaseDataPipeline[A] {
  override def map[B](f: A => B): DataPipeline[B] = new MappingPipeline[A, B](f, this)
  override def flatMap[B](f: A => Iterable[B]): DataPipeline[B] = new FlatMapPipeline[A, B](f, this)
  override def filter(p: A => Boolean): DataPipeline[A] = new CollectPipeline[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): DataPipeline[B] = new CollectPipeline[A, B](pf, this)
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = Runtime.getRuntime.availableProcessors())
                          (f: A => Future[B])
                          (implicit ec: ExecutionContext): DataPipeline[B] = {
    new MappingPipeline[A, B]({ a =>
      val future = f(a)
      Await.result(future, timeout)
    }, this)
  }
}

protected[trembita]
class StrictSource[+A](iterable: => Iterable[A]) extends SeqSource[A] {
  override def force: Iterable[A] = iterable
  override def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSource[A](iterable)
  override def take(n: Int): Iterable[A] = iterable.take(n).toVector
  override def drop(n: Int): Iterable[A] = iterable.drop(n).toVector
  override def headOption: Option[A] = iterable.headOption
  override def iterator: Iterator[A] = iterable.iterator

  override def :+[BB >: A](elem: BB): DataPipeline[BB] = new StrictSource(iterable ++ Some(elem))
  override def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(iterable ++ that.force)
}

protected[trembita]
class CachedSource[+A](iterable: Iterable[A]) extends SeqSource[A] {
  override def force: Iterable[A] = iterable
  override def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSource[A](iterable)
  override def take(n: Int): Iterable[A] = iterable.take(n).toVector
  override def drop(n: Int): Iterable[A] = iterable.drop(n).toVector
  override def headOption: Option[A] = iterable.headOption
  override def iterator: Iterator[A] = iterable.iterator

  override def :+[BB >: A](elem: BB): DataPipeline[BB] = new StrictSource(iterable ++ Some(elem))
  override def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(iterable ++ that.force)
}

protected[trembita]
class SortedSource[+A: Ordering : ClassTag](source: DataPipeline[A]) extends SeqSource[A] {
  override def sorted[B >: A : Ordering : ClassTag]: DataPipeline[B] = this
  override def force: Iterable[A] = {
    val forced: Iterable[A] = source.force
    forced.toVector.sorted
  }
  override def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSortedSource[A](this)
  override def iterator: Iterator[A] = this.force.iterator

  override def :+[BB >: A](elem: BB): DataPipeline[BB] = new StrictSource(this.force ++ Some(elem))
  override def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(this.force ++ that.force)
}
