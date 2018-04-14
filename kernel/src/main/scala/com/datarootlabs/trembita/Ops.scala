package com.datarootlabs.trembita


import scala.language.higherKinds
import cats._
import cats.implicits._
import scala.collection.generic.CanBuildFrom


trait Ops[A, F[_], T <: PipelineType] extends Any {
  def self: DataPipeline[A, F, T]

  def eval(implicit ev: T <:< PipelineType.Finite): F[Vector[A]] = self.evalFunc[A]
  def bind(f: Either[Throwable, A] ⇒ F[Unit]): F[Unit] = self.bindFunc[A](f)

  /**
    * Evaluates the pipeline
    * as an arbitrary collection
    *
    * @tparam Coll - type of the resulting collection
    * @param cbf - [[CanBuildFrom]] instance for the collection
    * @return - pipeline's values wrapped into [[Coll]]
    **/
  def evalAs[Coll[_]](implicit ev: T <:< PipelineType.Finite,
                      cbf: CanBuildFrom[Coll[A], A, Coll[A]],
                      F: Functor[F]): F[Coll[A]] = self.evalFunc.map { e ⇒
    val builder = cbf()
    builder ++= e
    builder.result()
  }
  /**
    * Reduces [[DataPipeline]]
    * into single element
    * having a [[Monoid]] defined for type [[A]]
    *
    * @param M - monoid for pipeline's elements
    * @return - combined elements
    **/
  def reduce(implicit M: Monoid[A], ev: T <:< PipelineType.Finite, F: Functor[F]): F[A] = self.evalFunc.map(M.combineAll)

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
  def reduce(f: (A, A) ⇒ A)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[A] =
    reduceOpt(f).map(_.get)

  /**
    * SAFE version of [[reduce]]
    * handling empty [[DataPipeline]]
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduceOpt(f: (A, A) ⇒ A)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Option[A]] =
    foldLeft(Option.empty[A]) {
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
  def foldLeft[C](zero: C)(f: (C, A) ⇒ C)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[C] =
    self.evalFunc.map(_.foldLeft(zero)(f))

  /**
    * @return - size of the [[DataPipeline]]
    **/
  def size(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Int] = foldLeft(0)((s, _) ⇒ s + 1)

  /**
    * Takes only first N elements of the pipeline
    *
    * @param n - number of elements to take
    * @return - first N elements
    **/
  def take(n: Int)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Vector[A]] =
    self.evalFunc.map(_.take(n))
  //
  //  /**
  //    * Take all elements of the pipeline
  //    * dropping first N elements
  //    *
  //    * @param n - number of elements to drop
  //    * @return - all elements with first N dropped
  //    **/
  def drop(n: Int)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Vector[A]] =
    self.evalFunc.map(_.drop(n))

  /**
    * Looks for the first element [[A]]
    * satisfying given predicate
    *
    * @param p - predicate
    * @return - {{{Some(element)}}} satisfying p or [[None]]
    **/
  def find(p: A ⇒ Boolean)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Option[A]] =
    self.evalFunc.map(_.find(p))

  /**
    * Checks an existence of some [[A]]
    * satisfying given predicate
    *
    * @param p - predicate
    * @return - the fact of existence of such [[A]]
    **/
  def exists(p: A ⇒ Boolean)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Boolean] =
    find(p).map(_.nonEmpty)

  /**
    * Checks an existence of some [[A]]
    * equal to the given object
    *
    * @param elem - object to compare with
    * @return - the fact of existence of such [[A]]
    **/
  def contains(elem: A)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Boolean] =
    exists(_ == elem)

  /**
    * Checks that each [[A]] in the pipeline
    * satisfies given predicate
    *
    * @param p - predicate
    * @return - {{{true}}} if yes and {{{false}}} otherwise
    **/
  def forall(p: A ⇒ Boolean)(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Boolean] =
    exists(!p(_)).map(!_)
  //
  /**
    * UNSAFE version of [[headOption]]
    *
    * @return - first element of the pipeline
    **/
  def head(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[A] = self.evalFunc.map(_.head)

  /**
    * @return - {{{Some(firstElement)}}} if pipeline is not empty, [[None]] otherwise
    **/
  def headOption(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): F[Option[A]] =
    self.evalFunc.map(_.headOption)
  //
  //  /**
  //    * Performs the given side effect
  //    * to each element of the pipeline
  //    *
  //    * @param f - side effect
  //    * @return - an [[Unit]]
  //    **/
  //  def foreach(f: A ⇒ Unit): F[Unit]
  //
  //  /**
  //    * @return - an [[Iterator]] with elements of the pipeline
  //    **/
  //  def iterator: Iterator[A]
  //
  //  /**
  //    * Returns a new [[DataPipeline]]
  //    * with all elements evaluated
  //    *
  //    * @return - a [[CachedPipeline]]
  //    **/
  //  def cache()(implicit ev: T <:< PipelineType.Finite, F: Functor[F]): DataPipeline[A, F, T] = new CachedPipeline(this.eval)
  //
  /**
    * Prints each element of the pipeline
    * as a side effect
    *
    * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
    **/
  def log(toString: A ⇒ String = (b: A) ⇒ b.toString): DataPipeline[A, F, T] = self.map { a ⇒ println(toString(a)); a }
  //
  //  /**
  //    * Splits a DataPipeline into N parts
  //    *
  //    * @param parts - N
  //    * @return - a pipeline of batches
  //    **/
  //  def batch(parts: Int)(implicit ev: T <:< PipelineType.Finite, F: Functor[F])
  //  : DataPipeline[Iterable[A], F, T] = new StrictSource[Iterable[A], F, T](evalFunc.map { vs ⇒
  //    ListUtils.batch(parts)(vs).toIterator
  //  })
  //
}
