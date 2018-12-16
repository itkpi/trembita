package com.github.trembita

import cats.{Monad, MonadError, ~>}
import com.github.trembita.internal._

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait ExecutionIndependentOps[F[_], A, Ex <: Execution] extends Any {
  def `this`: DataPipelineT[F, A, Ex]

  def map[B: ClassTag](
    magnet: Magnet[A, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapImpl[B](magnet.prepared)

  def flatMap[B: ClassTag](
    magnet: Magnet[A, DataPipelineT[F, B, Ex], Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.flatMapImpl[B](magnet.prepared)

  def collect[B: ClassTag](
    partialMagnet: PartialMagnet[A, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.collectImpl[B](partialMagnet.prepared)

  def flatCollect[B: ClassTag](
    partialMagnet: PartialMagnet[A, DataPipelineT[F, B, Ex], Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    collect(partialMagnet).flatten

  def flatten[B: ClassTag](implicit ev: A <:< DataPipelineT[F, B, Ex],
                           F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.flatMapImpl(ev)

  def handleError(magnet: Magnet[Throwable, A, Ex])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] = `this`.handleErrorImpl[A](magnet.prepared)

  def recover(magnet: PartialMagnet[Throwable, A, Ex])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] =
    `this`.handleErrorImpl[A](
      magnet.prepared.applyOrElse(_, (e: Throwable) => throw e)
    )

  def recoverNonFatal(magnet: Magnet[Throwable, A, Ex])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] =
    `this`.handleErrorImpl {
      case NonFatal(e) => magnet.prepared(e)
      case other       => throw other
    }

  def handleErrorWith(magnet: MagnetF[F, Throwable, A, Ex])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] = `this`.handleErrorWithImpl[A](magnet.prepared)

  def recoverWith(magnet: PartialMagnetF[F, Throwable, A, Ex])(
    implicit F: MonadError[F, Throwable],
    A: ClassTag[A]
  ): DataPipelineT[F, A, Ex] =
    `this`.handleErrorWithImpl[A](
      magnet.prepared.applyOrElse(_, e => F.raiseError[A](e))
    )

  def memoize()(implicit A: ClassTag[A], F: Monad[F]): DataPipelineT[F, A, Ex] =
    new MemoizedPipelineT[F, A, Ex](`this`, F)

  def mapM[B: ClassTag](
    magnet: MagnetF[F, A, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapMImpl[A, B](magnet.prepared)

  def mapG[B: ClassTag, G[_]](
    magnet: MagnetF[G, A, B, Ex]
  )(implicit funcK: G ~> F, F: Monad[F]): DataPipelineT[F, B, Ex] =
    `this`.mapMImpl[A, B] { a =>
      val gb = magnet.prepared(a)
      val fb = funcK(gb)
      fb
    }

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
    new GroupByPipelineT[F, K, A, Ex](f, `this`, F)

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
  def distinctBy[B: ClassTag](f: A => B)(implicit A: ClassTag[A],
                                         F: Monad[F]): DataPipelineT[F, A, Ex] =
    this.groupBy(f).mapImpl { case (_, group) => group.head }

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
      `this`.asInstanceOf[DataPipelineT[F, A, Ex]],
      F
    )

  def sortBy[B: Ordering](f: A => B)(implicit A: ClassTag[A],
                                     F: Monad[F]): DataPipelineT[F, A, Ex] =
    new SortedPipelineT[A, F, Ex](
      `this`.asInstanceOf[DataPipelineT[F, A, Ex]],
      F
    )(Ordering.by(f), A)

  def zip[B: ClassTag](
    that: DataPipelineT[F, B, Ex]
  )(implicit A: ClassTag[A], F: Monad[F]): DataPipelineT[F, (A, B), Ex] =
    new ZipPipelineT[F, A, B, Ex](`this`, that)

  def ++(that: DataPipelineT[F, A, Ex])(implicit A: ClassTag[A],
                                        F: Monad[F]): DataPipelineT[F, A, Ex] =
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

  def cartesian[B](
    that: DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, (A, B), Ex] =
    `this`.flatMapImpl { a =>
      that.mapImpl(b => a -> b)
    }

  def mapConcat[B, Repr[_]](magnet: Magnet[A, Repr[B], Ex])(
    implicit F: Monad[F],
    B: ClassTag[B],
    ev: Repr[B] => DataPipelineT[F, B, Ex]
  ): DataPipelineT[F, B, Ex] =
    `this`.flatMapImpl { a =>
      ev(magnet.prepared(a))
    }

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
