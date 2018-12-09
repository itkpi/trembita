package com.github.trembita

import cats._
import cats.implicits._
import com.github.trembita.internal._

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds

trait Ops0[A, F[_], Ex <: Execution] extends Any {
  def self: DataPipelineT[F, A, Ex]

  def eval(implicit Ex: Ex, F: Functor[F]): F[Vector[A]] =
    self.evalFunc[A](Ex).map(repr => Ex.toVector(repr.asInstanceOf[Ex.Repr[A]]))

  /**
    * Groups the pipeline using given grouping criteria.
    *
    * Returns a [[GroupByPipelineT]] - special implementation of [[DataPipelineT]]
    *
    * @tparam K - grouping criteria
    * @param f - function to extract [[K]] from [[A]]
    * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
    **/
  def groupBy[K](f: A => K)(implicit ex: Ex,
                            F: Monad[F]): DataPipelineT[F, (K, Vector[A]), Ex] =
    new GroupByPipelineT[F, K, A, Ex](f, self, ex, F)

  /**
    * Special case of [[distinctBy]]
    * Guarantees that each element of pipeline is unique
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    * implemented for type [[A]]
    *
    * @return - pipeline with only unique elements
    **/
  def distinct(implicit ex: Ex, F: Monad[F]): DataPipelineT[F, A, Ex] =
    distinctBy(identity)

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
  def distinctBy[B](f: A => B)(implicit ex: Ex,
                               F: Monad[F]): DataPipelineT[F, A, Ex] =
    this.groupBy(f).map { case (_, group) => group.head }

  /**
    * Orders elements of the [[DataPipelineT]]
    * having an [[Ordering]] defined for type [[A]]
    *
    * @return - the same pipeline sorted
    **/
  def sorted(implicit ex: Ex,
             F: Monad[F],
             ordering: Ordering[A]): DataPipelineT[F, A, Ex] =
    new SortedPipelineT[A, F, Ex](
      self.asInstanceOf[DataPipelineT[F, A, Ex]],
      ex,
      F
    )

  def sortBy[B: Ordering](f: A => B)(implicit ex: Ex,
                                     F: Monad[F]): DataPipelineT[F, A, Ex] =
    new SortedPipelineT[A, F, Ex](
      self.asInstanceOf[DataPipelineT[F, A, Ex]],
      ex,
      F
    )(Ordering.by(f))

  def memoize()(implicit ex: Ex, F: Monad[F]): DataPipelineT[F, A, Ex] =
    new MemoizedPipelineT[F, A, Ex](eval, F)

  /**
    * Evaluates the pipeline
    * as an arbitrary collection
    *
    * @tparam Coll - type of the resulting collection
    * @param cbf - [[CanBuildFrom]] instance for the collection
    * @return - pipeline's values wrapped into [[Coll]]
    **/
  def evalAs[Coll[_]](implicit Ex: Ex,
                      cbf: CanBuildFrom[Coll[A], A, Coll[A]],
                      F: Functor[F]): F[Coll[A]] = eval.map { e =>
    val builder = cbf()
    builder ++= e
    builder.result()
  }

  /**
    * Reduces [[DataPipelineT]]
    * into single element
    * having a [[Monoid]] defined for type [[A]]
    *
    * @param M - monoid for pipeline's elements
    * @return - combined elements
    **/
  def reduce(implicit M: Monoid[A], Ex: Ex, F: Functor[F]): F[A] =
    eval.map(M.combineAll)

  /**
    * UNSAFE version of [[reduceOpt]]
    *
    * Reduces [[DataPipelineT]]
    * into single element [[A]]
    * using reducing function
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduce(f: (A, A) => A)(implicit Ex: Ex, F: Functor[F]): F[A] =
    reduceOpt(f).map(_.get)

  /**
    * SAFE version of [[reduce]]
    * handling empty [[DataPipelineT]]
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduceOpt(f: (A, A) => A)(implicit Ex: Ex, F: Functor[F]): F[Option[A]] =
    foldLeft(Option.empty[A]) {
      case (None, b) => Some(b)
      case (a, b)    => a.map(f(_, b))
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
  def foldLeft[C](zero: C)(f: (C, A) => C)(implicit Ex: Ex,
                                           F: Functor[F]): F[C] =
    eval.map(_.foldLeft(zero)(f))

  /**
    * @return - size of the [[DataPipelineT]]
    **/
  def size(implicit F: Functor[F], Ex: Ex): F[Int] =
    foldLeft(0)((s, _) => s + 1)

  /**
    * Takes only first N elements of the pipeline
    *
    * @param n - number of elements to take
    * @return - first N elements
    **/
  def take(n: Int)(implicit Ex: Ex, F: Functor[F]): F[Vector[A]] =
    eval.map(_.take(n))

  //
  //  /**
  //    * Take all elements of the pipeline
  //    * dropping first N elements
  //    *
  //    * @param n - number of elements to drop
  //    * @return - all elements with first N dropped
  //    **/
  def drop(n: Int)(implicit Ex: Ex, F: Functor[F]): F[Vector[A]] =
    eval.map(_.drop(n))

  /**
    * Looks for the first element [[A]]
    * satisfying given predicate
    *
    * @param p - predicate
    * @return - {{{Some(element)}}} satisfying p or [[None]]
    **/
  def find(p: A => Boolean)(implicit Ex: Ex, F: Functor[F]): F[Option[A]] =
    eval.map(_.find(p))

  /**
    * Checks an existence of some [[A]]
    * satisfying given predicate
    *
    * @param p - predicate
    * @return - the fact of existence of such [[A]]
    **/
  def exists(p: A => Boolean)(implicit Ex: Ex, F: Functor[F]): F[Boolean] =
    find(p).map(_.nonEmpty)

  /**
    * Checks an existence of some [[A]]
    * equal to the given object
    *
    * @param elem - object to compare with
    * @return - the fact of existence of such [[A]]
    **/
  def contains(elem: A)(implicit Ex: Ex, F: Functor[F]): F[Boolean] =
    exists(_ == elem)

  /**
    * Checks that each [[A]] in the pipeline
    * satisfies given predicate
    *
    * @param p - predicate
    * @return - {{{true}}} if yes and {{{false}}} otherwise
    **/
  def forall(p: A => Boolean)(implicit Ex: Ex, F: Functor[F]): F[Boolean] =
    exists(!p(_)).map(!_)

  //
  /**
    * UNSAFE version of [[headOption]]
    *
    * @return - first element of the pipeline
    **/
  def head(implicit Ex: Ex, F: Functor[F]): F[A] = eval.map(_.head)

  /**
    * @return - {{{Some(firstElement)}}} if pipeline is not empty, [[None]] otherwise
    **/
  def headOption(implicit Ex: Ex, F: Functor[F]): F[Option[A]] =
    eval.map(_.headOption)
}
