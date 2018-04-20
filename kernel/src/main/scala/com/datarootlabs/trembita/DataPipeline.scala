package com.datarootlabs.trembita

import scala.language.higherKinds
import cats._
import cats.implicits._
import internal._
import scala.util.{Random, Success, Try}


/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipeline[+A, F[_], T <: Finiteness, Ex <: Execution] {
  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipeline]]
    **/
  def map[B](f: A ⇒ B): DataPipeline[B, F, T, Ex]

  /**
    * Monad.flatMap
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{DataPipeline[B]}}}
    * @return - transformed [[DataPipeline]]
    **/
  def flatMap[B](f: A ⇒ DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex]

  def flatten[B](implicit ev: A <:< DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex] = flatMap(ev)

  /**
    * Guarantees that [[DataPipeline]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipeline]]
    **/
  def filter(p: A ⇒ Boolean): DataPipeline[A, F, T, Ex]

  /**
    * Applies a [[PartialFunction]] to the [[DataPipeline]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipeline]]
    **/
  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B, F, T, Ex]

  def flatCollect[B](pf: PartialFunction[A, DataPipeline[B, F, T, Ex]]): DataPipeline[B, F, T, Ex] =
    collect(pf).flatten

  def mapM[B](f: A ⇒ F[B]): DataPipeline[B, F, T, Ex]

  def mapK[B, G[_]](f: A ⇒ G[B])(implicit funcK: G ~> F): DataPipeline[B, F, T, Ex]

  def handleError[B >: A](f: Throwable ⇒ B): DataPipeline[B, F, T, Ex]

  def handleErrorWith[B >: A](f: Throwable ⇒ DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex]

  /**
    * Forces evaluation of [[DataPipeline]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/

  protected[trembita] def evalFunc[B >: A](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[B]]

  protected[trembita] def consumeFunc[B >: A](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit]
}

object DataPipeline {
  /**
    * Wraps given elements into a [[DataPipeline]]
    *
    * @param xs - elements to wrap
    * @return - a [[StrictSource]]
    **/
  def apply[A](xs: A*): DataPipeline[A, Try, Finiteness.Finite, Execution.Sequential] =
    new StrictSource[A, Try, Finiteness.Finite, Execution.Sequential](Success(xs.toIterator))

  def applyF[A, F[_]](xs: A*)(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, Finiteness.Finite, Execution.Sequential] =
    new StrictSource[A, F, Finiteness.Finite, Execution.Sequential](xs.toIterator.pure[F])

  def infinite[A, F[_]](f: ⇒ A)(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, Finiteness.Infinite, Execution.Sequential] =
    new StrictSource[A, F, Finiteness.Infinite, Execution.Sequential](Iterator.continually(f).pure[F])

  def infiniteF[A, F[_]](f: ⇒ F[A])(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, Finiteness.Infinite, Execution.Sequential] =
    new StrictSource[F[A], F, Finiteness.Infinite, Execution.Sequential](Iterator.continually(f).pure[F]).mapM(identity)

  /**
    * Wraps an [[Iterable]] passed by-name
    *
    * @param it - an iterable haven't been evaluated yet
    * @return - a [[StrictSource]]
    **/
  def from[A](it: ⇒ Iterable[A]): DataPipeline[A, Try, Finiteness.Finite, Execution.Sequential] =
    new StrictSource[A, Try, Finiteness.Finite, Execution.Sequential](Try {
      it.toIterator
    })

  def fromEffect[A, F[_], T <: Finiteness, Ex <: Execution]
  (fa: F[Iterable[A]])
  (implicit F: MonadError[F, Throwable])
  : DataPipeline[A, F, T, Ex] =
    new StrictSource[A, F, T, Ex](fa.map(_.toIterator))

  /**
    * @return - an empty [[DataPipeline]]
    **/
  def empty[A]: DataPipeline[A, Try, Finiteness.Finite, Execution.Sequential] =
    new StrictSource[A, Try, Finiteness.Finite, Execution.Sequential](Success(Iterator.empty))

  /**
    * Creates a [[DataPipeline]]
    * containing the result of repeatable call of the given function
    *
    * @param times - size of the resulting pipeline
    * @param fa    - factory function
    * @return - data pipeline
    **/
  def repeat[A](times: Int)(fa: ⇒ A): DataPipeline[A, Try, Finiteness.Finite, Execution.Sequential] =
    new StrictSource(Success((1 to times).toIterator.map(_ ⇒ fa)))

  /**
    * Creates a [[DataPipeline]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts(size: Int): DataPipeline[Int, Try, Finiteness.Finite, Execution.Sequential] = repeat(size)(Random.nextInt())

  /**
    * Creates a [[DataPipeline]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @param max  - upper limit for resulting integers
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts(size: Int, max: Int): DataPipeline[Int, Try, Finiteness.Finite, Execution.Sequential] = repeat(size)(Random.nextInt(max))

  /**
    * Creates a [[DataPipeline]]
    * from lines of the given files
    *
    * @param fileName - file name
    * @return - pipeline with file lines as elements
    **/
  def fromFile(fileName: String): DataPipeline[String, Try, Finiteness.Finite, Execution.Sequential] = new StrictSource(
    Success(scala.io.Source.fromFile(fileName).getLines())
  )
}