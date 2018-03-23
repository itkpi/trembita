package com.datarootlabs.trembita.parallel


import cats.effect._
import cats.implicits._
import cats.effect.implicits._
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.internal._
import com.datarootlabs.trembita.utils._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.reflect.ClassTag


protected[trembita]
class ParMapPipeline[+A, B](f: A => B, source: DataPipeline[A])
                           (implicit val ec: ExecutionContext) extends MapingPipeline[A, B](f, source) with ParDataPipeline[B] {

  override def force: Iterable[B] = {
    val forced: Iterable[A] = source.force
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.split(ParDataPipeline.defaultParallelism)(forced).map { group =>
        Future {
          group.map(f)
        }
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  }

  override def runM[BB >: B, M[_]](implicit M: Sync[M]): M[Iterable[BB]] = {
    val forced: Iterable[A] = source.force
    ListUtils.split(ParDataPipeline.defaultParallelism)(forced)
      .mapM(group ⇒ M.delay(group.map(f)))
      .map(_.flatten)
  }

  override def seq: DataPipeline[B] = new MapingPipeline[A, B](f, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f2: B => Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = {
    new ParSource[B](this.force).mapAsync(timeout, parallelism)(f2)
  }
}


protected[trembita]
class ParFlatMapPipeline[+A, B](f: A => Iterable[B], source: DataPipeline[A])
                               (implicit val ec: ExecutionContext) extends FlatMapPipeline[A, B](f, source)
                                                                   with ParDataPipeline[B] {
  override def force: Iterable[B] = {
    val forced: Iterable[A] = source.force
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.split(ParDataPipeline.defaultParallelism)(forced).map { group =>
        Future {
          group.flatMap(f)
        }
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  }

  override def runM[BB >: B, M[_]](implicit M: Sync[M]): M[Iterable[BB]] = {
    val forced: Iterable[A] = source.force
    ListUtils.split(ParDataPipeline.defaultParallelism)(forced)
      .mapM(group ⇒ M.delay(group.flatMap(f)))
      .map(_.flatten)
  }

  override def seq: DataPipeline[B] = new FlatMapPipeline[A, B](f, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f2: B => Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = new ParSource[C]({
    val forced: Iterable[B] = this.force
    ListUtils.split(parallelism)(forced).flatMap { group =>
      val future = Future.sequence(group.map(f2))
      Await.result(future, timeout)
    }
  })
}


protected[trembita]
class ParCollectPipeline[+A, B](pf: PartialFunction[A, B], source: DataPipeline[A])
                               (implicit val ec: ExecutionContext) extends CollectPipeline[A, B](pf, source) with ParDataPipeline[B] {
  override def force: Iterable[B] = {
    val forced: Iterable[A] = source.force
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.split(ParDataPipeline.defaultParallelism)(forced).map { group =>
        Future {
          group.collect(pf)
        }
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  }

  override def runM[BB >: B, M[_]](implicit M: Sync[M]): M[Iterable[BB]] = {
    val forced: Iterable[A] = source.force
    ListUtils.split(ParDataPipeline.defaultParallelism)(forced)
      .mapM(group ⇒ M.delay(group.collect(pf)))
      .map(_.flatten)
  }

  override def seq: DataPipeline[B] = new ParCollectPipeline[A, B](pf, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f: B => Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = new ParSource[C]({
    val forced: Iterable[B] = this.force
    ListUtils.split(parallelism)(forced).flatMap { group =>
      val future = Future.sequence(group.map(f))
      Await.result(future, timeout)
    }
  })
}


protected[trembita]
class ParSource[+A](iterable: => Iterable[A])(implicit val ec: ExecutionContext) extends BaseDataPipeline[A] with ParDataPipeline[A] {
  override def map[B](f: A => B): DataPipeline[B] = new ParMapPipeline[A, B](f, this)
  override def flatMap[B](f: A => Iterable[B]): DataPipeline[B] = new ParFlatMapPipeline[A, B](f, this)
  override def filter(p: A => Boolean): DataPipeline[A] = new ParCollectPipeline[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): DataPipeline[B] = new ParCollectPipeline[A, B](pf, this)
  override def force: Iterable[A] = iterable
  override def seq: DataPipeline[A] = new StrictSource[A](iterable)
  override def iterator: Iterator[A] = iterable.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f: A => Future[B])
                          (implicit ec: ExecutionContext): DataPipeline[B] = new ParSource[B]({
    val forced: Iterable[A] = iterable
    ListUtils.split(parallelism)(forced).flatMap { group =>
      val future = Future.sequence(group.map(f))
      Await.result(future, timeout)
    }
  })
}


protected[trembita]
class ParSortedSource[+A: Ordering : ClassTag](source: DataPipeline[A])
                                              (implicit val ec: ExecutionContext) extends BaseDataPipeline[A] with ParDataPipeline[A] {
  override def map[B](f: A => B): DataPipeline[B] = new ParMapPipeline[A, B](f, this)
  override def flatMap[B](f: A => Iterable[B]): DataPipeline[B] = new ParFlatMapPipeline[A, B](f, this)
  override def filter(p: A => Boolean): DataPipeline[A] = new CollectPipeline[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): DataPipeline[B] = new CollectPipeline[A, B](pf, this)
  override def force: Iterable[A] = source match {
    case s: SortedSource[A] => s.force
    case _                  =>
      val forced: Array[A] = source.force.toArray
      MergeSort.sort(forced)
      forced
  }

  override def seq: DataPipeline[A] = new SortedSource[A](source)
  override def iterator: Iterator[A] = this.force.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f: A => Future[B])
                          (implicit ec: ExecutionContext): DataPipeline[B] = {
    new ParSource[A](this.force).mapAsync(timeout, parallelism)(f)
  }
}

