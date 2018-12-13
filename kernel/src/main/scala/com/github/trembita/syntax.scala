package com.github.trembita

import cats._
import cats.implicits._
import com.github.trembita.internal._

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.reflect.ClassTag

object syntax extends AllSyntax {}

trait AllSyntax {
  implicit def liftOps[A, F[_], Ex <: Execution](
    self: DataPipelineT[F, A, Ex]
  )(implicit ex: Ex): ExDependentOps[A, F, Ex] =
    new ExDependentOps(self)(ex)

  final class ExDependentOps[A, F[_], Ex <: Execution](self: DataPipelineT[F, A, Ex])(
    implicit final val Ex: Ex
  ) extends Serializable {

    def eval(implicit F: Functor[F], run: Ex.Run[F]): F[Vector[A]] =
      self
        .evalFunc[A](Ex)
        .map(repr => Ex.toVector(repr.asInstanceOf[Ex.Repr[A]]))

    /**
      * Groups the pipeline using given grouping criteria.
      *
      * Returns a [[GroupByPipelineT]] - special implementation of [[DataPipelineT]]
      *
      * @tparam K - grouping criteria
      * @param f - function to extract [[K]] from [[A]]
      * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
      **/
    def groupBy[K: ClassTag](f: A => K)(
      implicit A: ClassTag[A],
      F: Monad[F]
    ): DataPipelineT[F, (K, Iterable[A]), Ex] =
      new GroupByPipelineT[F, K, A, Ex](f, self, F)

    /**
      * Special case of [[distinctBy]]
      * Guarantees that each element of pipeline is unique
      *
      * CONTRACT: the caller is responsible for correct {{{equals}}}
      * implemented for type [[A]]
      *
      * @return - pipeline with only unique elements
      **/
    def distinct(implicit
                 A: ClassTag[A],
                 F: Monad[F]): DataPipelineT[F, A, Ex] =
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
    def distinctBy[B: ClassTag](
      f: A => B
    )(implicit A: ClassTag[A], F: Monad[F]): DataPipelineT[F, A, Ex] =
      this.groupBy(f).map { case (_, group) => group.head }

    /**
      * Orders elements of the [[DataPipelineT]]
      * having an [[Ordering]] defined for type [[A]]
      *
      * @return - the same pipeline sorted
      **/
    def sorted(implicit F: Monad[F],
               A: ClassTag[A],
               ordering: Ordering[A]): DataPipelineT[F, A, Ex] =
      new SortedPipelineT[A, F, Ex](
        self.asInstanceOf[DataPipelineT[F, A, Ex]],
        F
      )

    def sortBy[B: Ordering](f: A => B)(implicit A: ClassTag[A],
                                       F: Monad[F]): DataPipelineT[F, A, Ex] =
      new SortedPipelineT[A, F, Ex](
        self.asInstanceOf[DataPipelineT[F, A, Ex]],
        F
      )(Ordering.by(f), A)

    def memoize()(implicit A: ClassTag[A],
                  F: Monad[F],
                  run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
      new MemoizedPipelineT[F, A, Ex](
        self.evalFunc(Ex).asInstanceOf[F[Ex#Repr[A]]],
        F
      )

    /**
      * Evaluates the pipeline
      * as an arbitrary collection
      *
      * @tparam Coll - type of the resulting collection
      * @param cbf - [[CanBuildFrom]] instance for the collection
      * @return - pipeline's values wrapped into [[Coll]]
      **/
    def evalAs[Coll[_]](implicit
                        cbf: CanBuildFrom[Coll[A], A, Coll[A]],
                        F: Functor[F],
                        run: Ex.Run[F]): F[Coll[A]] = eval.map { e =>
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
    def reduce(implicit M: Monoid[A], run: Ex.Run[F], F: Functor[F]): F[A] =
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
    def reduce(f: (A, A) => A)(implicit run: Ex.Run[F], F: Functor[F]): F[A] =
      reduceOpt(f).map(_.get)

    /**
      * SAFE version of [[reduce]]
      * handling empty [[DataPipelineT]]
      *
      * @param f - reducing function
      * @return - combined elements
      **/
    def reduceOpt(f: (A, A) => A)(implicit run: Ex.Run[F],
                                  F: Functor[F]): F[Option[A]] =
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
    def foldLeft[C](zero: C)(f: (C, A) => C)(implicit run: Ex.Run[F],
                                             F: Functor[F]): F[C] =
      eval.map(_.foldLeft(zero)(f))

    /**
      * @return - size of the [[DataPipelineT]]
      **/
    def size(implicit F: Functor[F], run: Ex.Run[F]): F[Int] =
      foldLeft(0)((s, _) => s + 1)

    /**
      * Takes only first N elements of the pipeline
      *
      * @param n - number of elements to take
      * @return - first N elements
      **/
    def take(n: Int)(implicit run: Ex.Run[F], F: Functor[F]): F[Vector[A]] =
      eval.map(_.take(n))

    //
    //  /**
    //    * Take all elements of the pipeline
    //    * dropping first N elements
    //    *
    //    * @param n - number of elements to drop
    //    * @return - all elements with first N dropped
    //    **/
    def drop(n: Int)(implicit run: Ex.Run[F], F: Functor[F]): F[Vector[A]] =
      eval.map(_.drop(n))

    /**
      * Looks for the first element [[A]]
      * satisfying given predicate
      *
      * @param p - predicate
      * @return - {{{Some(element)}}} satisfying p or [[None]]
      **/
    def find(p: A => Boolean)(implicit run: Ex.Run[F],
                              F: Functor[F]): F[Option[A]] =
      eval.map(_.find(p))

    /**
      * Checks an existence of some [[A]]
      * satisfying given predicate
      *
      * @param p - predicate
      * @return - the fact of existence of such [[A]]
      **/
    def exists(p: A => Boolean)(implicit run: Ex.Run[F],
                                F: Functor[F]): F[Boolean] =
      find(p).map(_.nonEmpty)

    /**
      * Checks an existence of some [[A]]
      * equal to the given object
      *
      * @param elem - object to compare with
      * @return - the fact of existence of such [[A]]
      **/
    def contains(elem: A)(implicit run: Ex.Run[F], F: Functor[F]): F[Boolean] =
      exists(_ == elem)

    /**
      * Checks that each [[A]] in the pipeline
      * satisfies given predicate
      *
      * @param p - predicate
      * @return - {{{true}}} if yes and {{{false}}} otherwise
      **/
    def forall(p: A => Boolean)(implicit run: Ex.Run[F],
                                F: Functor[F]): F[Boolean] =
      exists(!p(_)).map(!_)

    //
    /**
      * UNSAFE version of [[headOption]]
      *
      * @return - first element of the pipeline
      **/
    def head(implicit run: Ex.Run[F], F: Functor[F]): F[A] =
      eval.map(_.head)

    /**
      * @return - {{{Some(firstElement)}}} if pipeline is not empty, [[None]] otherwise
      **/
    def headOption(implicit
                   run: Ex.Run[F],
                   F: Functor[F]): F[Option[A]] =
      eval.map(_.headOption)

    def to[Ex2 <: Execution](implicit run1: Ex.Run[F],
                             A: ClassTag[A],
                             F: Monad[F]): DataPipelineT[F, A, Ex2] =
      BridgePipelineT.make[F, A, Ex, Ex2](self, Ex, F)

    /**
      * Prints each element of the pipeline
      * as a side effect
      *
      * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
      **/
    def log(
      toString: A => String = (b: A) => b.toString
    )(implicit F: Monad[F], A: ClassTag[A]): DataPipelineT[F, A, Ex] =
      self.map { a =>
        println(toString(a)); a
      }

    //    def join[B](that: DataPipelineT[F, B, Ex.type])(
    //      on: (A, B) => Boolean
    //    )(implicit F: Monad[F], run: Ex.Run[F]): DataPipelineT[F, (A, B), Ex] =
    //      new StrictSource[F, (A, B), Ex]({
    //        for {
    //          self <- eval
    //          that <- that.eval
    //        } yield {
    //          self.iterator.flatMap { a =>
    //            that.collect {
    //              case b if on(a, b) => a -> b
    //            }
    //          }
    //        }
    //      }, F)
    //
    //    def joinLeft[B](that: DataPipelineT[F, B, Ex])(on: (A, B) => Boolean)(
    //      implicit F: Monad[F],
    //      run: Ex.Run[F]
    //    ): DataPipelineT[F, (A, Option[B]), Ex] =
    //      new StrictSource[F, (A, Option[B]), Ex]({
    //        for {
    //          self <- self.eval
    //          that <- that.eval
    //        } yield {
    //          self.iterator.flatMap { a =>
    //            that.collect {
    //              case b if on(a, b) => a -> Some(b)
    //              case _             => a -> None
    //            }
    //          }
    //        }
    //      }, F)
    //
    //    def joinRight[B](that: DataPipelineT[F, B, Ex])(on: (A, B) => Boolean)(
    //      implicit F: Monad[F],
    //      run: Ex.Run[F]
    //    ): DataPipelineT[F, (Option[A], B), Ex] =
    //      new StrictSource[F, (Option[A], B), Ex]({
    //        for {
    //          self <- self.eval
    //          that <- that.eval
    //        } yield {
    //          that.iterator.flatMap { b =>
    //            self.collect {
    //              case a if on(a, b) => Some(a) -> b
    //              case _             => None -> b
    //            }
    //          }
    //        }
    //      }, F)
    //
    //    def cartesian[B](
    //      that: DataPipelineT[F, B, Ex]
    //    )(implicit F: Monad[F]): DataPipelineT[F, (A, B), Ex] =
    //      for {
    //        a <- self
    //        b <- that
    //      } yield a -> b
    //
    //    def ++(that: DataPipelineT[F, A, Ex])(
    //      implicit F: Monad[F],
    //      A: ClassTag[A],
    //      run: Ex.Run[F]
    //    ): DataPipelineT[F, A, Ex] =
    //      new StrictSource[F, A, Ex](for {
    //        self <- self.eval
    //        that <- that.eval
    //      } yield self.iterator ++ that.iterator, F)
    //
    //    def zip[B](
    //      that: DataPipelineT[F, B, Ex]
    //    )(implicit F: Monad[F], run: Ex.Run[F]): DataPipelineT[F, (A, B), Ex] =
    //      new StrictSource[F, (A, B), Ex](for {
    //        self <- self.eval
    //        that <- that.eval
    //      } yield self.iterator zip that.iterator, F)
    //
    //    def zipWithIndex(implicit F: Monad[F],
    //                     run: Ex.Run[F]): DataPipelineT[F, (A, Int), Ex] =
    //      new StrictSource[F, (A, Int), Ex](
    //        self.eval.map(_.toIterator.zipWithIndex),
    //        F
    //      )
    //
    //    def :++(fa: F[A])(implicit F: Monad[F],
    //                      A: ClassTag[A],
    //                      run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    //      self ++ DataPipelineT.liftF[F, A, Ex](fa.map(List(_)))
    //
    //    def :+(a: A)(implicit F: Monad[F],
    //                 A: ClassTag[A],
    //                 run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    //      self :++ F.pure(a)
    //
    //    def ++:(fa: F[A])(implicit F: Monad[F],
    //                      A: ClassTag[A],
    //                      run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    //      DataPipelineT.liftF[F, A, Ex](fa.map(List(_))) ++ self
    //
    //    def +:(a: A)(implicit F: Monad[F],
    //                 A: ClassTag[A],
    //                 run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    //      F.pure(a) ++: self
    //
    //    def mapK[G[_]]()(implicit funcK: F ~> G,
    //                     A: ClassTag[A],
    //                     F: Functor[F],
    //                     G: Monad[G],
    //                     run: Ex.Run[F]): DataPipelineT[G, A, Ex] =
    //      new StrictSource[G, A, Ex](funcK(self.eval).map(_.iterator), G)
  }
}
