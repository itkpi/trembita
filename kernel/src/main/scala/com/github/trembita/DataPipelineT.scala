package com.github.trembita

import scala.language.higherKinds
import cats._
import cats.effect.Sync
import cats.implicits._
import internal._

import scala.reflect.ClassTag
import scala.util.{Random, Success, Try}

/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipelineT[F[_], +A, Ex <: Execution] extends Serializable {

  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipelineT]]
    **/
  protected[trembita] def mapImpl[B: ClassTag](f: A => B)(
    implicit F: Monad[F]
  ): DataPipelineT[F, B, Ex]

  /**
    * Monad.flatMap
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{DataPipeline[B]}}}
    * @return - transformed [[DataPipelineT]]
    **/
  protected[trembita] def flatMapImpl[B: ClassTag](
    f: A => DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex]

  /**
    * Guarantees that [[DataPipelineT]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipelineT]]
    **/
  protected[trembita] def filterImpl[AA >: A](
    p: A => Boolean
  )(implicit F: Monad[F], A: ClassTag[AA]): DataPipelineT[F, AA, Ex] =
    collectImpl[AA]({ case a if p(a) => a })

  /**
    * Applies a [[PartialFunction]] to the [[DataPipelineT]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipelineT]]
    **/
  protected[trembita] def collectImpl[B: ClassTag](pf: PartialFunction[A, B])(
    implicit F: Monad[F]
  ): DataPipelineT[F, B, Ex]

  protected[trembita] def mapMImpl[AA >: A, B: ClassTag](
    f: A => F[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new MapMonadicPipelineT[F, A, B, Ex](f, this)(F)

  protected[trembita] def handleErrorImpl[B >: A: ClassTag](f: Throwable => B)(
    implicit F: MonadError[F, Throwable]
  ): DataPipelineT[F, B, Ex]

  protected[trembita] def handleErrorWithImpl[B >: A: ClassTag](
    f: Throwable => F[B]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex]

  /**
    * Forces evaluation of [[DataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](Ex: Ex)(
    implicit run: Ex.Run[F]
  ): F[Ex.Repr[B]]
}

object DataPipelineT {
  def apply[F[_], A: ClassTag](
    xs: A*
  )(implicit F: Monad[F]): DataPipelineT[F, A, Execution.Sequential] =
    new StrictSource[F, A](xs.toIterator.pure[F], F)

  def liftF[F[_], A: ClassTag, Ex <: Execution](
    fa: F[Iterable[A]]
  )(implicit liftPipeline: LiftPipeline[F, Ex]): DataPipelineT[F, A, Ex] =
    liftPipeline.liftIterableF(fa)

  /**
    * @return - an empty [[DataPipelineT]]
    **/
  def empty[F[_], A: ClassTag](
    implicit F: Monad[F]
  ): DataPipelineT[F, A, Execution.Sequential] =
    new StrictSource[F, A](F.pure(Iterator.empty), F)

  /**
    * Creates a [[DataPipelineT]]
    * containing the result of repeatable call of the given function
    *
    * @param times - size of the resulting pipeline
    * @param fa    - factory function
    * @return - data pipeline
    **/
  def repeat[F[_], A: ClassTag](
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
