package com.github.trembita.operations
import cats.{~>, Monad, MonadError}
import com.github.trembita.internal._
import com.github.trembita.{operations, DataPipelineT, Environment}
import scala.language.higherKinds
import scala.reflect.ClassTag

trait EnvironmentIndependentOps[F[_], A, Ex <: Environment] extends Any {
  def `this`: DataPipelineT[F, A, Ex]

  def flatten[B: ClassTag](implicit ev: A <:< Iterable[B], F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapConcatImpl(ev)

  def memoize()(implicit A: ClassTag[A], F: Monad[F]): DataPipelineT[F, A, Ex] =
    new MemoizedPipelineT[F, A, Ex](`this`, F)

  /**
    * Groups the pipeline using given grouping criteria.
    *
    * Returns a [[GroupByPipelineT]] - special implementation of [[DataPipelineT]]
    *
    * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
    **/
  def groupBy[K: ClassTag](f: A => K)(
      implicit canGroupBy: CanGroupBy[Ex#Repr],
      F: Monad[F],
      A: ClassTag[A]
  ): DataPipelineT[F, (K, Iterable[A]), Ex] = GroupByPipelineT.make[F, K, A, Ex](f, `this`, F, canGroupBy)

  def groupByOrdered[K: ClassTag: Ordering](f: A => K)(
      implicit canGroupByOrdered: CanGroupByOrdered[Ex#Repr],
      F: Monad[F],
      A: ClassTag[A]
  ): DataPipelineT[F, (K, Iterable[A]), Ex] = GroupByOrderedPipelineT.make[F, K, A, Ex](f, `this`, F, canGroupByOrdered)

  /**
    * Special case of [[distinctBy]]
    * Guarantees that each element of pipeline is unique
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    * implemented for type [[A]]
    *
    * @return - pipeline with only unique elements
    **/
  def distinct(implicit canGroupBy: CanGroupBy[Ex#Repr], A: ClassTag[A], F: Monad[F]): DataPipelineT[F, A, Ex] =
    distinctBy(identity)

  /**
    * Guarantees that each element of pipeline is unique
    * according to the given criteria
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    *
    * @return - pipeline with only unique elements
    **/
  def distinctBy[K: ClassTag](f: A => K)(implicit canGroupBy: CanGroupBy[Ex#Repr], F: Monad[F], A: ClassTag[A]): DataPipelineT[F, A, Ex] =
    groupBy(f).mapImpl(_._2.head)

  def zip[B: ClassTag](
      that: DataPipelineT[F, B, Ex]
  )(implicit A: ClassTag[A], F: Monad[F], canZip: CanZip[Ex#Repr]): DataPipelineT[F, (A, B), Ex] =
    new ZipPipelineT[F, A, B, Ex](`this`, that, canZip)

  def ++(that: DataPipelineT[F, A, Ex])(implicit A: ClassTag[A], F: Monad[F]): DataPipelineT[F, A, Ex] =
    new ConcatPipelineT[F, A, Ex](`this`, that)

  def join[B](that: DataPipelineT[F, B, Ex])(on: (A, B) => Boolean)(
      implicit canJoin: CanJoin[Ex#Repr],
      A: ClassTag[A],
      B: ClassTag[B],
      F: Monad[F]
  ): DataPipelineT[F, (A, B), Ex] =
    new JoinPipelineT[F, A, B, Ex](`this`, that, on)

  def joinLeft[B](that: DataPipelineT[F, B, Ex])(on: (A, B) => Boolean)(
      implicit canJoin: CanJoin[Ex#Repr],
      A: ClassTag[A],
      B: ClassTag[B],
      F: Monad[F]
  ): DataPipelineT[F, (A, Option[B]), Ex] =
    new JoinLeftPipelineT[F, A, B, Ex](`this`, that, on)

  def joinRight[B](that: DataPipelineT[F, B, Ex])(on: (A, B) => Boolean)(
      implicit canJoin: CanJoin[Ex#Repr],
      A: ClassTag[A],
      B: ClassTag[B],
      F: Monad[F]
  ): DataPipelineT[F, (Option[A], B), Ex] =
    new JoinRightPipelineT[F, A, B, Ex](`this`, that, on)

  /**
    * Prints each element of the pipeline
    * as a side effect
    *
    * @param toString - extract [[String]] representation of [[A]] (defaults to [[AnyRef.toString]])
    **/
  def log(
      toString: A => String = (b: A) => b.toString
  )(implicit F: Monad[F], A: ClassTag[A]): DataPipelineT[F, A, Ex] =
    `this`.mapImpl { a =>
      println(toString(a)); a
    }
}
