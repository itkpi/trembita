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


/** Basic operations implemented for all [[ParDataPipeline]]s */
protected[trembita]
trait BaseParPipeline[+A] extends ParDataPipeline[A] {
  def take(n: Int): Iterable[A] = this.eval.take(n)
  def drop(n: Int): Iterable[A] = this.eval.drop(n)
  def find[C >: A](p: C ⇒ Boolean): Option[C] = this.eval.find(p)
  def headOption: Option[A] = this.eval.headOption
  def iterator: Iterator[A] = this.eval.iterator

  def :+[C >: A](elem: C): DataPipeline[C] = new ParSource[C](this.eval ++ Some(elem))
  def ++[C >: A](that: DataPipeline[C]): DataPipeline[C] = new ParSource[C](this.eval ++ that.eval)
}

/** Parallel version of [[MappingPipeline]] */
protected[trembita]
class ParMapPipeline[+A, B](f: A ⇒ B, source: DataPipeline[A])
                           (implicit val ec: ExecutionContext) extends BaseParPipeline[B] {

  /**
    * Evaluates an [[Iterable]] of [[B]]
    * splitting the [[source]] on [[ParDataPipeline.defaultParallelism]] parts
    * and mapping each part in parallel using [[ec]]
    **/
  override def eval: Iterable[B] = {
    val forced: Iterable[A] = source.eval
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.batch(ParDataPipeline.defaultParallelism)(forced).map { group ⇒
        Future {
          group.map(f)
        }
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  }

  /**
    * Does the same as [[eval]]
    * but wraps it into [[Sync]]
    *
    * @tparam M - some [[Sync]] (see Effect)
    * @param M - an [[Sync]] implementation for [[M]]
    * @return - evaluated iterable wrapped into [[M]]
    **/
  override protected[trembita] def runM[BB >: B, M[_]](implicit M: Sync[M]): M[Iterable[BB]] = {
    val forced: Iterable[A] = source.eval
    ListUtils.batch(ParDataPipeline.defaultParallelism)(forced)
      .mapM(group ⇒ M.delay(group.map(f)))
      .map(_.flatten)
  }

  override def seq: DataPipeline[B] = new MappingPipeline[A, B](f, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f2: B ⇒ Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = new ParSource[C]({
    val forced: Iterable[A] = source.eval
    val futureRes: Future[Iterable[Iterable[C]]] = Future.sequence {
      ListUtils.batch(parallelism)(forced).map { group ⇒
        Future.sequence(group.map(f.andThen(f2)))
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  })

  /** --------------------------------------------------------------
    * The following functions mimics [[MappingPipeline]] functions
    * returning parallel implementations
    * -------------------------------------------------------------- */
  def map[C](f2: B ⇒ C): ParMapPipeline[A, C] = new ParMapPipeline(f.andThen(f2), source)
  def flatMap[C](f2: B ⇒ Iterable[C]): ParFlatMapPipeline[A, C] = new ParFlatMapPipeline[A, C](f.andThen(f2), source)
  def filter(p: B ⇒ Boolean): ParFlatMapPipeline[A, B] = new ParFlatMapPipeline[A, B](a ⇒ Some(f(a)).filter(p), source)
  def collect[C](pf: PartialFunction[B, C]): ParFlatMapPipeline[A, C] =
    new ParFlatMapPipeline[A, C](a ⇒ Some(f(a)).collect(pf), source)
}


/** Parallel version of [[FlatMapPipeline]] */
protected[trembita]
class ParFlatMapPipeline[+A, B](f: A ⇒ Iterable[B], source: DataPipeline[A])
                               (implicit val ec: ExecutionContext) extends BaseParPipeline[B] {
  /**
    * Evaluates an [[Iterable]] of [[B]]
    * splitting the [[source]] on [[ParDataPipeline.defaultParallelism]] parts
    * and flatMapping each part in parallel using [[ec]]
    **/
  override def eval: Iterable[B] = {
    val forced: Iterable[A] = source.eval
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.batch(ParDataPipeline.defaultParallelism)(forced).map { group ⇒
        Future {
          group.flatMap(f)
        }
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  }

  /**
    * Does the same as [[eval]]
    * but wraps it into [[Sync]]
    *
    * @tparam M - some [[Sync]] (see Effect)
    * @param M - an [[Sync]] implementation for [[M]]
    * @return - evaluated iterable wrapped into [[M]]
    **/
  override protected[trembita] def runM[BB >: B, M[_]](implicit M: Sync[M]): M[Iterable[BB]] = {
    val forced: Iterable[A] = source.eval
    ListUtils.batch(ParDataPipeline.defaultParallelism)(forced)
      .mapM(group ⇒ M.delay(group.flatMap(f)))
      .map(_.flatten)
  }

  override def seq: DataPipeline[B] = new FlatMapPipeline[A, B](f, source)

  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f2: B ⇒ Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = new ParSource[C]({
    val forced: Iterable[A] = source.eval
    val futureRes: Future[Iterable[Iterable[C]]] = Future.sequence {
      ListUtils.batch(parallelism)(forced).map { group ⇒
        Future.sequence(group.flatMap(f(_).map(f2)))
      }
    }
    Await.result(futureRes, timeout).flatten
  })

  /** --------------------------------------------------------------
    * The following functions mimics [[FlatMapPipeline]] functions
    * returning parallel implementations
    * -------------------------------------------------------------- */
  def map[C](f2: B ⇒ C): ParFlatMapPipeline[A, C] = new ParFlatMapPipeline[A, C](f(_).map(f2), source)
  def flatMap[C](f2: B ⇒ Iterable[C]): ParFlatMapPipeline[A, C] = new ParFlatMapPipeline[A, C](f(_).flatMap(f2), source)
  def filter(p: B ⇒ Boolean): ParFlatMapPipeline[A, B] = new ParFlatMapPipeline[A, B](f(_).filter(p), source)
  def collect[C](pf: PartialFunction[B, C]): ParFlatMapPipeline[A, C] = new ParFlatMapPipeline[A, C](f(_).collect(pf), source)
}


protected[trembita]
class ParCollectPipeline[+A, B](pf: PartialFunction[A, B], source: DataPipeline[A])
                               (implicit val ec: ExecutionContext) extends BaseParPipeline[B] {
  override def eval: Iterable[B] = {
    val forced: Iterable[A] = source.eval
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.batch(ParDataPipeline.defaultParallelism)(forced).map { group ⇒
        Future {
          group.collect(pf)
        }
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  }

  override protected[trembita] def runM[BB >: B, M[_]](implicit M: Sync[M]): M[Iterable[BB]] = {
    val forced: Iterable[A] = source.eval
    ListUtils.batch(ParDataPipeline.defaultParallelism)(forced)
      .mapM(group ⇒ M.delay(group.collect(pf)))
      .map(_.flatten)
  }

  override def seq: DataPipeline[B] = new ParCollectPipeline[A, B](pf, source)
  override def mapAsync[C](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f: B ⇒ Future[C])
                          (implicit ec: ExecutionContext): DataPipeline[C] = new ParSource[C]({
    val forced: Iterable[B] = this.eval
    val futureRes: Future[Iterable[Iterable[C]]] = Future.sequence {
      ListUtils.batch(parallelism)(forced).map { group ⇒
        Future.sequence(group.map(f))
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  })

  /** --------------------------------------------------------------
    * The following functions mimics [[CollectPipeline]] functions
    * returning parallel implementations
    * -------------------------------------------------------------- */
  def map[C](f2: B ⇒ C): ParCollectPipeline[A, C] = new ParCollectPipeline[A, C](pf.andThen(f2), source)
  def flatMap[C](f2: B ⇒ Iterable[C]): ParFlatMapPipeline[A, C] =
    new ParFlatMapPipeline[A, C](a ⇒ Some(a).collect(pf).toList.flatMap(f2), source)

  def filter(p: B ⇒ Boolean): ParFlatMapPipeline[A, B] = new ParFlatMapPipeline[A, B](a ⇒ Some(a).collect(pf).filter(p), source)
  def collect[C](pf2: PartialFunction[B, C]): ParCollectPipeline[A, C] = new ParCollectPipeline[A, C](pf.andThen(pf2), source)
}


/**
  * Parallel version of [[StrictSource]]
  **/
protected[trembita]
class ParSource[+A](iterable: ⇒ Iterable[A])(implicit val ec: ExecutionContext) extends BaseParPipeline[A] {
  override def map[B](f: A ⇒ B): ParMapPipeline[A, B] = new ParMapPipeline[A, B](f, this)
  override def flatMap[B](f: A ⇒ Iterable[B]): ParFlatMapPipeline[A, B] = new ParFlatMapPipeline[A, B](f, this)
  override def filter(p: A ⇒ Boolean): ParDataPipeline[A] = new ParCollectPipeline[A, A](
    {
      case a if p(a) ⇒ a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): ParCollectPipeline[A, B] = new ParCollectPipeline[A, B](pf, this)
  override def eval: Iterable[A] = iterable
  override def seq: DataPipeline[A] = new StrictSource[A](iterable)
  override def iterator: Iterator[A] = iterable.iterator
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f: A ⇒ Future[B])
                          (implicit ec: ExecutionContext): ParSource[B] = new ParSource[B]({
    val forced: Iterable[A] = iterable
    val futureRes: Future[Iterable[Iterable[B]]] = Future.sequence {
      ListUtils.batch(parallelism)(forced).map { group ⇒
        Future.sequence(group.map(f))
      }
    }
    Await.result(futureRes, ParDataPipeline.defaultTimeout).flatten
  })
}


/**
  * Parallel version of [[SortedPipeline]]
  * Uses parallel [[MergeSort]]
  **/
protected[trembita]
class ParSortedPipeline[+A: Ordering : ClassTag](source: DataPipeline[A])
                                                (implicit val ec: ExecutionContext) extends BaseParPipeline[A] {
  override def map[B](f: A ⇒ B): ParMapPipeline[A, B] = new ParMapPipeline[A, B](f, this)
  override def flatMap[B](f: A ⇒ Iterable[B]): ParFlatMapPipeline[A, B] = new ParFlatMapPipeline[A, B](f, this)
  override def filter(p: A ⇒ Boolean): ParDataPipeline[A] = new ParCollectPipeline[A, A](
    {
      case a if p(a) ⇒ a
    },
    this
  )
  override def collect[B](pf: PartialFunction[A, B]): ParCollectPipeline[A, B] = new ParCollectPipeline[A, B](pf, this)
  override def eval: Iterable[A] = source match {
    case s: SortedPipeline[A]     ⇒ s.eval
    case ps: ParSortedPipeline[A] ⇒ ps.eval
    case _                        ⇒
      val forced: Array[A] = source.eval.toArray
      MergeSort.sort(forced)
      forced.toVector
  }

  override def seq: SortedPipeline[A] = new SortedPipeline[A](this)
  override def mapAsync[B](timeout: FiniteDuration,
                           parallelism: Int = ParDataPipeline.defaultParallelism)
                          (f: A ⇒ Future[B])
                          (implicit ec: ExecutionContext): ParSource[B] = {
    new ParSource[A](this.eval).mapAsync(timeout, parallelism)(f)
  }
}

