package com.github.trembita

import scala.language.higherKinds
import cats._
import cats.effect.Sync
import cats.implicits._
import internal._

import scala.util.{Random, Success, Try}

/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipelineT[F[_], +A, Ex <: Execution] {

  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipelineT]]
    **/
  def map[B](f: A => B)(implicit F: Monad[F]): DataPipelineT[F, B, Ex]

  /**
    * Monad.flatMap
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{DataPipeline[B]}}}
    * @return - transformed [[DataPipelineT]]
    **/
  def flatMap[B](f: A => DataPipelineT[F, B, Ex])(
    implicit F: Monad[F]
  ): DataPipelineT[F, B, Ex]

  def flatten[B](implicit ev: A <:< DataPipelineT[F, B, Ex],
                 F: Monad[F]): DataPipelineT[F, B, Ex] = flatMap(ev)

  /**
    * Guarantees that [[DataPipelineT]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipelineT]]
    **/
  def filter(p: A => Boolean)(implicit F: Monad[F]): DataPipelineT[F, A, Ex]

  /**
    * Applies a [[PartialFunction]] to the [[DataPipelineT]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipelineT]]
    **/
  def collect[B](pf: PartialFunction[A, B])(
    implicit F: Monad[F]
  ): DataPipelineT[F, B, Ex]

  def flatCollect[B](
    pf: PartialFunction[A, DataPipelineT[F, B, Ex]]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    collect(pf).flatten

  def mapM[B](f: A => F[B])(implicit F: Monad[F]): DataPipelineT[F, B, Ex]

  def mapK[B, G[_]](f: A => G[B])(
    implicit funcK: G ~> F
  ): DataPipelineT[F, B, Ex]

  def handleError[B >: A](f: Throwable => B)(
    implicit F: MonadError[F, Throwable]
  ): DataPipelineT[F, B, Ex]

  def handleErrorWith[B >: A](f: Throwable => DataPipelineT[F, B, Ex])(
    implicit F: MonadError[F, Throwable]
  ): DataPipelineT[F, B, Ex]

  /**
    * Forces evaluation of [[DataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](Ex: Ex): F[Ex.Repr[B]]
}

object DataPipelineT {
  def apply[F[_], A](
    xs: A*
  )(implicit F: Monad[F]): DataPipelineT[F, A, Execution.Sequential] =
    new StrictSource[F, A, Execution.Sequential](xs.toIterator.pure[F], F)

  def liftF[F[_], A, Ex <: Execution](
    fa: F[Iterable[A]]
  )(implicit F: Monad[F]): DataPipelineT[F, A, Ex] =
    new StrictSource[F, A, Ex](fa.map(_.toIterator), F)

  /**
    * @return - an empty [[DataPipelineT]]
    **/
  def empty[F[_], A](
    implicit F: Monad[F]
  ): DataPipelineT[F, A, Execution.Sequential] =
    new StrictSource[F, A, Execution.Sequential](F.pure(Iterator.empty), F)

  /**
    * Creates a [[DataPipelineT]]
    * containing the result of repeatable call of the given function
    *
    * @param times - size of the resulting pipeline
    * @param fa    - factory function
    * @return - data pipeline
    **/
  def repeat[F[_], A](
    times: Int
  )(fa: => A)(implicit F: Sync[F]): DataPipelineT[F, A, Execution.Sequential] =
    new StrictSource(F.delay(1 to times).map(_.toIterator.map(_ => fa)), F)

  /**
    * Creates a [[DataPipelineT]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts[F[_]](
    size: Int
  )(implicit F: Sync[F]): DataPipelineT[F, Int, Execution.Sequential] =
    repeat(size)(Random.nextInt())

  /**
    * Creates a [[DataPipelineT]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @param max  - upper limit for resulting integers
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts[F[_]](size: Int, max: Int)(
    implicit F: Sync[F]
  ): DataPipelineT[F, Int, Execution.Sequential] =
    repeat(size)(Random.nextInt(max))

  /**
    * Creates a [[DataPipelineT]]
    * from lines of the given files
    *
    * @param fileName - file name
    * @return - pipeline with file lines as elements
    **/
  def fromFile[F[_]](
    fileName: String
  )(implicit F: Sync[F]): DataPipelineT[F, String, Execution.Sequential] =
    new StrictSource(F.delay(scala.io.Source.fromFile(fileName).getLines()), F)
}
