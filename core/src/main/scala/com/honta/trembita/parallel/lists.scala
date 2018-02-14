package com.honta.trembita.parallel


import com.honta.trembita.{BaseLazyList, LazyList}
import scala.concurrent.{Await, ExecutionContext, Future}
import com.honta.trembita.internal._
import scala.concurrent.duration._
import scala.reflect.ClassTag


protected[trembita] class ParMapList[+A, B](f: A => B, source: LazyList[A])
                                                  (implicit val ec: ExecutionContext) extends MapList[A, B](f, source) with ParLazyList[B] {

  override def force: Iterable[B] = {
    val forced: Iterable[A] = source.force
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.split(ParLazyList.defaultParallelism)(forced).map { group =>
        Future {
          group.map(f)
        }
      }
    }
    Await.result(futureRes, ParLazyList.defaultTimeout).flatten
  }

  override def seq: LazyList[B] = new MapList[A, B](f, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParLazyList.defaultParallelism)
                          (f2: B => Future[C])
                          (implicit ec: ExecutionContext): LazyList[C] = {
    new ParSource[B](this.force).mapAsync(timeout, parallelism)(f2)
  }
}


protected[trembita] class ParFlatMapList[+A, B](f: A => Iterable[B], source: LazyList[A])
                                                      (implicit val ec: ExecutionContext) extends FlatMapList[A, B](f, source)
  with ParLazyList[B] {
  override def force: Iterable[B] = {
    val forced: Iterable[A] = source.force
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.split(ParLazyList.defaultParallelism)(forced).map { group =>
        Future {
          group.flatMap(f)
        }
      }
    }
    Await.result(futureRes, ParLazyList.defaultTimeout).flatten
  }
  override def seq: LazyList[B] = new FlatMapList[A, B](f, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParLazyList.defaultParallelism)
                          (f2: B => Future[C])
                          (implicit ec: ExecutionContext): LazyList[C] = new ParSource[C]({
    val forced: Iterable[B] = this.force
    ListUtils.split(parallelism)(forced).flatMap { group =>
      val future = Future.sequence(group.map(f2))
      Await.result(future, timeout)
    }
  })
}


protected[trembita] class ParCollectList[+A, B](pf: PartialFunction[A, B], source: LazyList[A])
                                                      (implicit val ec: ExecutionContext) extends CollectList[A, B](pf, source) with ParLazyList[B] {
  override def force: Iterable[B] = {
    val forced: Iterable[A] = source.force
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.split(ParLazyList.defaultParallelism)(forced).map { group =>
        Future {
          group.collect(pf)
        }
      }
    }
    Await.result(futureRes, ParLazyList.defaultTimeout).flatten
  }
  override def seq: LazyList[B] = new ParCollectList[A, B](pf, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParLazyList.defaultParallelism)
                          (f: B => Future[C])
                          (implicit ec: ExecutionContext): LazyList[C] = new ParSource[C]({
    val forced: Iterable[B] = this.force
    ListUtils.split(parallelism)(forced).flatMap { group =>
      val future = Future.sequence(group.map(f))
      Await.result(future, timeout)
    }
  })
}


protected[trembita] class ParSource[+A](iterable: => Iterable[A])(implicit val ec: ExecutionContext) extends BaseLazyList[A] with ParLazyList[A] {
  override def map[B](f: A => B): LazyList[B] = new ParMapList[A, B](f, this)
  override def flatMap[B](f: A => Iterable[B]): LazyList[B] = new ParFlatMapList[A, B](f, this)
  override def filter(p: A => Boolean): LazyList[A] = new ParCollectList[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): LazyList[B] = new ParCollectList[A, B](pf, this)
  override def force: Iterable[A] = iterable
  override def seq: LazyList[A] = new StrictSource[A](iterable)
  override def iterator: Iterator[A] = iterable.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = ParLazyList.defaultParallelism)
                          (f: A => Future[B])
                          (implicit ec: ExecutionContext): LazyList[B] = new ParSource[B]({
    val forced: Iterable[A] = iterable
    ListUtils.split(parallelism)(forced).flatMap { group =>
      val future = Future.sequence(group.map(f))
      Await.result(future, timeout)
    }
  })
}


protected[trembita] class ParSortedSource[+A: Ordering : ClassTag](source: LazyList[A])
                                                                         (implicit val ec: ExecutionContext) extends BaseLazyList[A] with ParLazyList[A] {
  override def map[B](f: A => B): LazyList[B] = new ParMapList[A, B](f, this)
  override def flatMap[B](f: A => Iterable[B]): LazyList[B] = new ParFlatMapList[A, B](f, this)
  override def filter(p: A => Boolean): LazyList[A] = new CollectList[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): LazyList[B] = new CollectList[A, B](pf, this)
  override def force: Iterable[A] = source match {
    case s: SortedSource[A] => s.force
    case _                  =>
      val forced: Array[A] = source.force.toArray
      MergeSort.sort(forced)
      forced
  }

  override def seq: LazyList[A] = new SortedSource[A](source)
  override def iterator: Iterator[A] = this.force.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = ParLazyList.defaultParallelism)
                          (f: A => Future[B])
                          (implicit ec: ExecutionContext): LazyList[B] = {
    new ParSource[A](this.force).mapAsync(timeout, parallelism)(f)
  }
}

