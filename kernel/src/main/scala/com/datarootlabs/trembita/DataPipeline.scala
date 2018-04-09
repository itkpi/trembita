package com.datarootlabs.trembita

import scala.language.higherKinds
import cats.Monoid
import cats.data.Kleisli
import internal._
import parallel._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Random, Try}
import cats.effect._

import scala.collection.generic.CanBuildFrom


/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipeline[+A] {
  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipeline]]
    **/
  def map[B](f: A ⇒ B): DataPipeline[B]

  /**
    * Monad.flatMap
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{DataPipeline[B]}}}
    * @return - transformed [[DataPipeline]]
    **/
  def flatMap[B](f: A ⇒ Iterable[B]): DataPipeline[B]

  /**
    * Guarantees that [[DataPipeline]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipeline]]
    **/
  def filter(p: A ⇒ Boolean): DataPipeline[A]

  /**
    * Applies a [[PartialFunction]] to the [[DataPipeline]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipeline]]
    **/
  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B]

  /**
    * Applies transformation wrapped into [[Kleisli]]
    *
    * @tparam C - resulting data type
    * @param flow - set of transformation to be applied
    * @return - transformed [[DataPipeline]]
    *
    **/
  def transform[B >: A, C](flow: Kleisli[DataPipeline, DataPipeline[B], C]): DataPipeline[C] = flow.run(this)

  /**
    * Forces evaluation of [[DataPipeline]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  def eval: Iterable[A]

  /**
    * Evaluates the pipeline
    * as an arbitrary collection
    *
    * @tparam Coll - type of the resulting collection
    * @param cbf - [[CanBuildFrom]] instance for the collection
    * @return - pipeline's values wrapped into [[Coll]]
    **/
  protected[trembita]
  def evalAsColl[B >: A, Coll[_]](implicit cbf: CanBuildFrom[Coll[B], B, Coll[B]]): Coll[B] = {
    val builder = cbf()
    builder ++= eval
    builder.result()
  }
  /**
    * Forces evaluation of [[DataPipeline]]
    * collecting data into [[Iterable]]
    * within a [[Sync]] (see Effect)
    *
    * @tparam M - implementation of [[cats.effect.Sync]]
    * @return - collected data wrapped into [[M]]
    **/
  protected[trembita]
  def runM[B >: A, M[_]](implicit M: Sync[M]): M[Iterable[B]] = M.delay(eval)

  /**
    * Reduces [[DataPipeline]]
    * into single element
    * having a [[Monoid]] defined for type [[A]]
    *
    * @param M - monoid for pipeline's elements
    * @return - combined elements
    **/
  def reduce[B >: A](implicit M: Monoid[B]): B = M.combineAll(this.eval)

  /**
    * UNSAFE version of [[reduceOpt]]
    *
    * Reduces [[DataPipeline]]
    * into single element [[A]]
    * using reducing function
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduce[B >: A](f: (B, B) ⇒ B): B = reduceOpt(f).get

  /**
    * SAFE version of [[reduce]]
    * handling empty [[DataPipeline]]
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduceOpt[B >: A](f: (B, B) ⇒ B): Option[B] = foldLeft(Option.empty[B]) {
    case (None, b) ⇒ Some(b)
    case (a, b)    ⇒ a.map(f(_, b))
  }

  /**
    * Left oriented fold function
    * returning a single instance of [[C]]
    *
    * @tparam C - type of the combiner
    * @param zero - initial combiner
    * @param f    - function for 'adding' a value of type [[A]] to the combiner [[C]]
    * @return - a single instance of the combiner [[C]]
    **/
  def foldLeft[C](zero: C)(f: (C, A) ⇒ C): C = {
    val forced: Iterable[A] = this.eval
    forced.foldLeft(zero)(f)
  }

  /**
    * @return - size of the [[DataPipeline]]
    **/
  def size: Int = foldLeft(0)((s, _) ⇒ s + 1)

  /**
    * Orders elements of the [[DataPipeline]]
    * having an [[Ordering]] defined for type [[A]]
    *
    * @return - the same pipeline sorted
    **/
  def sorted[B >: A : Ordering : ClassTag]: DataPipeline[B]

  /**
    * Special case of [[distinctBy]]
    * Guarantees that each element of pipeline is unique
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    * implemented for type [[A]]
    *
    * @return - pipeline with only unique elements
    **/
  def distinct: DataPipeline[A] = new StrictSource[A](this.eval.toSet)

  /**
    * Guarantees that each element of pipeline is unique
    * according to the given criteria
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    * implemented for type [[B]]
    *
    * @tparam B - uniqueness criteria type
    * @param f - function to extract [[B]] from the pipeline element
    * @return - pipeline with only unique elements
    **/
  def distinctBy[B](f: A ⇒ B): DataPipeline[A] = this.groupBy(f).map { case (_, group) ⇒ group.head }

  /**
    * Returns a parallel version of this pipeline
    *
    * Each next transformation on the [[DataPipeline]]
    * will be performed in parallel using given [[ExecutionContext]]
    *
    * @param ec - an execution context
    * @return - A [[ParDataPipeline]] with the same elements
    **/
  def par(implicit ec: ExecutionContext): ParDataPipeline[A]

  /**
    * Returns a sequential version of this pipeline
    *
    * Each next transformation on the [[DataPipeline]]
    * will be performed sequentially
    *
    * @return - sequential pipeline with the same elements
    **/
  def seq: DataPipeline[A]

  /**
    * Groups the pipeline using given grouping criteria.
    *
    * Returns a [[GroupByPipeline]] - special implementation of [[DataPipeline]]
    *
    * @tparam K - grouping criteria
    * @param f - function to extract [[K]] from [[A]]
    * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
    **/
  def groupBy[K](f: A ⇒ K): DataPipeline[(K, Iterable[A])] = new GroupByPipeline[K, A](f, this)

  /**
    * Takes only first N elements of the pipeline
    *
    * @param n - number of elements to take
    * @return - first N elements
    **/
  def take(n: Int): Iterable[A]

  /**
    * Take all elements of the pipeline
    * dropping first N elements
    *
    * @param n - number of elements to drop
    * @return - all elements with first N dropped
    **/
  def drop(n: Int): Iterable[A]

  /**
    * Looks for the first element [[A]]
    * satisfying given predicate
    *
    * @param p - predicate
    * @return - {{{Some(element)}}} satisfying p or [[None]]
    **/
  def find[B >: A](p: B ⇒ Boolean): Option[B]

  /**
    * Checks an existence of some [[A]]
    * satisfying given predicate
    *
    * @param p - predicate
    * @return - the fact of existence of such [[A]]
    **/
  def exists(p: A ⇒ Boolean): Boolean = find(p).nonEmpty

  /**
    * Checks an existence of some [[A]]
    * equal to the given object
    *
    * @param elem - object to compare with
    * @return - the fact of existence of such [[A]]
    **/
  def contains[B >: A](elem: B): Boolean = find(_ == elem).nonEmpty

  /**
    * Checks that each [[A]] in the pipeline
    * satisfies given predicate
    *
    * @param p - predicate
    * @return - {{{true}}} if yes and {{{false}}} otherwise
    **/
  def forall(p: A ⇒ Boolean): Boolean = find(!p(_)).isEmpty

  /**
    * UNSAFE version of [[headOption]]
    *
    * @return - first element of the pipeline
    **/
  def head: A = headOption.get

  /**
    * @return - {{{Some(firstElement)}}} if pipeline is not empty, [[None]] otherwise
    **/
  def headOption: Option[A]

  /**
    * Performs the given side effect
    * to each element of the pipeline
    *
    * @param f - side effect
    * @return - an [[Unit]]
    **/
  def foreach(f: A ⇒ Unit): Unit = this.eval.foreach(f)

  /**
    * @return - an [[Iterator]] with elements of the pipeline
    **/
  def iterator: Iterator[A]

  /**
    * Returns a new [[DataPipeline]]
    * with all elements evaluated
    *
    * @return - a [[CachedPipeline]]
    **/
  def cache(): DataPipeline[A] = new CachedPipeline(this.eval)

  /**
    * Prints each element of the pipeline
    * as a side effect
    *
    * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
    **/
  def log[B >: A](toString: B ⇒ String = (b: B) ⇒ b.toString): DataPipeline[A] = this.map { a ⇒ println(toString(a)); a }

  /**
    * Special case of [[map]]
    *
    * @tparam B - resulting data type
    * @param f -  function from [[A]] to [[Try]] of [[B]]
    * @return - transformed pipeline
    **/
  def tryMap[B](f: A ⇒ Try[B]): DataPipeline[B] = this.flatMap(a ⇒ f(a).toOption)

  /**
    * Special case of [[map]]
    *
    * @tparam B - resulting type
    * @param timeout     - time to wait for evaluation of [[B]]
    * @param parallelism - number of threads to use for all transformations (defaults to [[ParDataPipeline.defaultParallelism]])
    * @param f           - asynchronous transformation
    * @param ec          - execution context
    * @return - transformed pipeline
    **/
  def mapAsync[B](timeout: FiniteDuration,
                  parallelism: Int = ParDataPipeline.defaultParallelism)
                 (f: A ⇒ Future[B])
                 (implicit ec: ExecutionContext): DataPipeline[B]

  /**
    * Pushes the given element into pipeline
    *
    * @param elem - element to push
    * @return - pipeline containing the given element
    **/
  def :+[B >: A](elem: B): DataPipeline[B]

  /**
    * Concatenates the pipeline with the second one
    *
    * @param that - some other pipeline
    * @return - this and that pipelines contatenated
    **/
  def ++[B >: A](that: DataPipeline[B]): DataPipeline[B]

  /**
    * Splits a DataPipeline into N parts
    *
    * @param parts - N
    * @return - a pipeline of batches
    **/
  def batch(parts: Int): DataPipeline[Iterable[A]] = new StrictSource[Iterable[A]](ListUtils.batch(parts)(eval))
}


/**
  * Base trait for sequential [[DataPipeline]]
  **/
protected[trembita] trait BaseDataPipeline[+A] extends DataPipeline[A] {
  def seq: DataPipeline[A] = this

  /** Uses sequential sorting algorithm */
  def sorted[B >: A : Ordering : ClassTag]: DataPipeline[B] = new SortedPipeline[B](this)
  def take(n: Int): Iterable[A] = this.eval.take(n)
  def drop(n: Int): Iterable[A] = this.eval.drop(n)
  def headOption: Option[A] = this.eval.headOption
  def find[B >: A](p: B ⇒ Boolean): Option[B] = this.eval.find(p)
}

object DataPipeline {
  /**
    * Wraps given elements into a [[DataPipeline]]
    *
    * @param xs - elements to wrap
    * @return - a [[StrictSource]]
    **/
  def apply[A](xs: A*): DataPipeline[A] = new StrictSource[A](xs)

  /**
    * Wraps an [[Iterable]] passed by-name
    *
    * @param it - an iterable haven't been evaluated yet
    * @return - a [[StrictSource]]
    **/
  def from[A](it: ⇒ Iterable[A]): DataPipeline[A] = new StrictSource[A](it)

  /**
    * @return - an empty [[DataPipeline]]
    **/
  def empty[A]: DataPipeline[A] = new StrictSource[A](Nil)

  /**
    * Creates a [[DataPipeline]]
    * containing the result of repeatable call of the given function
    *
    * @param times - size of the resulting pipeline
    * @param fa    - factory function
    * @return - data pipeline
    **/
  def repeat[A](times: Int)(fa: ⇒ A): DataPipeline[A] = new StrictSource[A]((1 to times).map(_ ⇒ fa))

  /**
    * Creates a [[DataPipeline]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts(size: Int): DataPipeline[Int] = repeat(size)(Random.nextInt())

  /**
    * Creates a [[DataPipeline]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @param max  - upper limit for resulting integers
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts(size: Int, max: Int): DataPipeline[Int] = repeat(size)(Random.nextInt(max))

  /**
    * Creates a [[DataPipeline]]
    * from lines of the given files
    *
    * @param fileName - file name
    * @return - pipeline with file lines as elements
    **/
  def fromFile(fileName: String): DataPipeline[String] = new StrictSource[String](
    scala.io.Source.fromFile(fileName).getLines().toIterable
  )
}