package com.datarootlabs.trembita.internal


import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag


/**
  * A [[DataPipeline]]
  * that was mapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita]
class MappingPipeline[+A, B](f: A => B, source: DataPipeline[A]) extends BaseDataPipeline[B] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C] = new MappingPipeline[A, C](f2.compose(f), source)

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => Iterable[C]): FlatMapPipeline[A, C] = {
    new FlatMapPipeline[A, C](f2.compose(f), source)
  }

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): FlatMapPipeline[A, B] = new FlatMapPipeline[A, B](
    a => Some(f(a)).filter(p),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf: PartialFunction[B, C]): FlatMapPipeline[A, C] = new FlatMapPipeline[A, C](
    a => Some(f(a)).collect(pf),
    source
  )

  /** Applies [[f]] to the [[source]] evaluated */
  def eval: Iterable[B] = source.eval.map(f)

  /** Returns [[ParMapPipeline]] */
  def par(implicit ec: ExecutionContext): ParMapPipeline[A, B] = new ParMapPipeline[A, B](f, source)

  def iterator: Iterator[B] = source.iterator.map(f)

  /** Ignores parallelism because the pipeline is not parallel */
  def mapAsync[C](timeout: FiniteDuration,
                  parallelism: Int = Runtime.getRuntime.availableProcessors())
                 (f2: B => Future[C])(implicit ec: ExecutionContext): MappingPipeline[A, C] =
    new MappingPipeline[A, C]({ a =>
      val future = f2(f(a))
      Await.result(future, timeout)
    }, source)

  def :+[BB >: B](elem: BB): DataPipeline[BB] = new StrictSource(this.eval ++ Some(elem))
  def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource[BB](this.eval ++ that.eval)
}

/**
  * A [[DataPipeline]]
  * that was flatMapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita]
class FlatMapPipeline[+A, B](f: A => Iterable[B], source: DataPipeline[A]) extends BaseDataPipeline[B] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): FlatMapPipeline[A, C] = {
    new FlatMapPipeline[A, C](
      a => f(a).map(f2),
      source
    )
  }

  /** Each next flatMap will compose [[f]] with some other map function */
  def flatMap[C](f2: B => Iterable[C]): FlatMapPipeline[A, C] = {
    new FlatMapPipeline[A, C](
      a => f(a).flatMap(f2),
      source
    )
  }

  /** Filters the result of [[f]] application */
  def filter(p: B => Boolean): FlatMapPipeline[A, B] = new FlatMapPipeline[A, B](
    a => f(a).filter(p),
    source
  )

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collect[C](pf: PartialFunction[B, C]): FlatMapPipeline[A, C] = new FlatMapPipeline[A, C](
    a => f(a).collect(pf),
    source
  )

  def eval: Iterable[B] = source.eval.flatMap(f)
  /** Return [[ParFlatMapPipeline]] */
  def par(implicit ec: ExecutionContext): ParDataPipeline[B] = new ParFlatMapPipeline[A, B](f, source)
  def iterator: Iterator[B] = source.eval.iterator.flatMap(f)

  /** Ignores parallelism because the pipeline is not parallel */
  def mapAsync[C](timeout: FiniteDuration,
                  parallelism: Int = Runtime.getRuntime.availableProcessors())
                 (f2: B => Future[C])(implicit ec: ExecutionContext): FlatMapPipeline[A, C] = {
    new FlatMapPipeline[A, C]({ a =>
      val future = Future.sequence(f(a).map(f2))
      Await.result(future, timeout)
    }, source)
  }

  def :+[BB >: B](elem: BB): DataPipeline[BB] = new StrictSource(this.eval ++ List(elem))
  def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(this.eval ++ that.eval)
}


/**
  * A [[DataPipeline]]
  * that was collected
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after pf application
  * @param pf     - [[PartialFunction]]
  * @param source - a pipeline that f was applied on
  **/
protected[trembita]
class CollectPipeline[+A, B](pf: PartialFunction[A, B], source: DataPipeline[A]) extends BaseDataPipeline[B] {
  def map[C](f: B => C): CollectPipeline[A, C] = new CollectPipeline[A, C](
    {
      case a if pf isDefinedAt a => f(pf(a))
    },
    source
  )
  def flatMap[BB](f: B => Iterable[BB]): FlatMapPipeline[A, BB] = {
    new FlatMapPipeline[A, BB](
      a =>
        if (pf isDefinedAt a) f(pf(a))
        else Iterable.empty,
      source
    )
  }

  def filter(p: B => Boolean): CollectPipeline[A, B] = new CollectPipeline[A, B](
    pf.andThen {
      case b if p(b) => b
    },
    source
  )
  def collect[BB](pf2: PartialFunction[B, BB]): CollectPipeline[A, BB] = new CollectPipeline[A, BB](
    pf.andThen(pf2),
    source
  )

  def eval: Iterable[B] = source.eval.collect(pf)
  /** Returns [[ParCollectPipeline]] */
  def par(implicit ec: ExecutionContext): ParCollectPipeline[A, B] = new ParCollectPipeline[A, B](pf, source)

  def iterator: Iterator[B] = source.eval.iterator.collect(pf)

  /** Ignores parallelism because the pipeline is not parallel */
  def mapAsync[C](timeout: FiniteDuration,
                  parallelism: Int = Runtime.getRuntime.availableProcessors())
                 (f: B => Future[C])
                 (implicit ec: ExecutionContext): CollectPipeline[A, C] = {
    new CollectPipeline[A, C]({
      case a if pf.isDefinedAt(a) =>
        val future = f(pf(a))
        Await.result(future, timeout)
    }, source)
  }

  def :+[BB >: B](elem: BB): DataPipeline[BB] = new StrictSource(this.eval ++ Some(elem))
  def ++[BB >: B](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource[BB](this.eval ++ that.eval)
}


/**
  * [[DataPipeline]] subclass
  * with basic operations implemented:
  *
  * [[DataPipeline.map]]      ~> [[MappingPipeline]]
  * [[DataPipeline.flatMap]]  ~> [[FlatMapPipeline]]
  * [[DataPipeline.collect]]  ~> [[CollectPipeline]]
  * [[DataPipeline.mapAsync]] ~> [[MappingPipeline]]
  **/
protected[trembita]
trait SeqSource[+A] extends BaseDataPipeline[A] {
  def map[B](f: A => B): MappingPipeline[A, B] = new MappingPipeline[A, B](f, this)
  def flatMap[B](f: A => Iterable[B]): FlatMapPipeline[A, B] = new FlatMapPipeline[A, B](f, this)
  def filter(p: A => Boolean): DataPipeline[A] = new CollectPipeline[A, A](
    {
      case a if p(a) => a
    },
    this
  )
  def collect[B](pf: PartialFunction[A, B]): CollectPipeline[A, B] = new CollectPipeline[A, B](pf, this)
  def mapAsync[B](timeout: FiniteDuration,
                  parallelism: Int = Runtime.getRuntime.availableProcessors())
                 (f: A => Future[B])
                 (implicit ec: ExecutionContext): MappingPipeline[A, B] = {
    new MappingPipeline[A, B]({ a =>
      val future = f(a)
      Await.result(future, timeout)
    }, this)
  }
}

/**
  * Concrete implementation of [[DataPipeline]]
  * wrapping by-name [[Iterable]]
  *
  * @tparam A - type of pipeline elements
  * @param iterable - not evaluated yet collection of [[A]]
  **/
protected[trembita]
class StrictSource[+A](iterable: => Iterable[A]) extends SeqSource[A] {
  /** Force evaluation of [[iterable]] */
  def eval: Iterable[A] = iterable

  /** Returns [[ParSource]] */
  def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSource[A](iterable)

  override def take(n: Int): Iterable[A] = iterable.take(n).toVector
  override def drop(n: Int): Iterable[A] = iterable.drop(n).toVector
  override def headOption: Option[A] = iterable.headOption

  def iterator: Iterator[A] = iterable.iterator
  def :+[BB >: A](elem: BB): DataPipeline[BB] = new StrictSource(iterable ++ Some(elem))
  def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(iterable ++ that.eval)
}

/**
  * Same to [[StrictSource]]
  * but wrapped [[Iterable]] already been evaluated
  *
  * @tparam A - type of pipeline elements
  * @param iterable -  result of [[DataPipeline]] transformations
  **/
protected[trembita]
class CachedPipeline[+A](iterable: Iterable[A]) extends StrictSource[A](iterable) {
}

/**
  * A [[DataPipeline]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
protected[trembita]
class SortedPipeline[+A: Ordering : ClassTag](source: DataPipeline[A]) extends SeqSource[A] {

  /** [[SortedPipeline.sorted]] = this */
  override def sorted[B >: A : Ordering : ClassTag]: SortedPipeline[B] = this

  /** Uses scala collection's sort algorithm */
  def eval: Iterable[A] = {
    val forced: Iterable[A] = source.eval
    forced.toVector.sorted
  }

  def par(implicit ec: ExecutionContext): ParDataPipeline[A] = new ParSource(this.eval)
  def iterator: Iterator[A] = this.eval.iterator

  def :+[BB >: A](elem: BB): DataPipeline[BB] = new StrictSource(this.eval ++ Some(elem))
  def ++[BB >: A](that: DataPipeline[BB]): DataPipeline[BB] = new StrictSource(this.eval ++ that.eval)
}
