package com.datarootlabs.trembita

import scala.language.higherKinds
import cats._
import cats.implicits._
import cats.data.Kleisli
import internal._
import parallel._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.{Random, Success, Try}
import cats.effect._

import collection.immutable
import scala.collection.generic.CanBuildFrom


sealed trait PipelineType
object PipelineType {
  sealed trait Finite extends PipelineType
  sealed trait Infinite extends PipelineType
  sealed trait Both extends Finite with Infinite

  sealed trait Concat[T1 <: PipelineType, T2 <: PipelineType] {
    type Out <: PipelineType
  }
  object Concat {
    implicit object FinitePlusFinite extends Concat[Finite, Finite] {
      type Out = Finite
    }
    implicit object InfinitePlusInfinite extends Concat[Infinite, Infinite] {
      type Out = Infinite
    }
    implicit object FinitePlusInfinite extends Concat[Finite, Infinite] {
      type Out = Infinite
    }
  }

  sealed trait Zip[T1 <: PipelineType, T2 <: PipelineType] {
    type Out <: PipelineType
  }
  object Zip {
    implicit def anyWithFinite[T1 <: PipelineType]: Zip[T1, Finite] = new Zip[T1, Finite] {
      type Out = Finite
    }
    implicit def finiteWithAny[T2 <: PipelineType]: Zip[Finite, T2] = new Zip[Finite, T2] {
      type Out = Finite
    }
    implicit object InfiniteWithInfinite extends Zip[Infinite, Infinite] {
      type Out = Infinite
    }
  }
}

/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipeline[+A, F[_], T <: PipelineType] {
  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipeline]]
    **/
  def map[B](f: A ⇒ B): DataPipeline[B, F, T]

  /**
    * Monad.flatMap
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{DataPipeline[B]}}}
    * @return - transformed [[DataPipeline]]
    **/
  def flatMap[B](f: A ⇒ DataPipeline[B, F, T]): DataPipeline[B, F, T]
  def flatten[B](implicit ev: A <:< DataPipeline[B, F, T]): DataPipeline[B, F, T] = flatMap(ev)
  /**
    * Guarantees that [[DataPipeline]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipeline]]
    **/
  def filter(p: A ⇒ Boolean): DataPipeline[A, F, T]

  /**
    * Applies a [[PartialFunction]] to the [[DataPipeline]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipeline]]
    **/
  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B, F, T]

  def flatCollect[B](pf: PartialFunction[A, DataPipeline[B, F, T]]): DataPipeline[B, F, T] =
    collect(pf).flatten

  def mapM[B](f: A ⇒ F[B]): DataPipeline[B, F, T]

  def mapK[B, G[_]](f: A ⇒ G[B])(implicit funcK: G ~> F): DataPipeline[B, F, T]
  /**
    * Applies transformation wrapped into [[Kleisli]]
    *
    * @tparam C - resulting data type
    * @param flow - set of transformation to be applied
    * @return - transformed [[DataPipeline]]
    *
    **/
  def transform[B >: A, C](flow: Flow[B, C, F, T]): DataPipeline[C, F, T] = flow.run(this)

  def handleError[B >: A](f: Throwable ⇒ B): DataPipeline[B, F, T]

  def handleErrorWith[B >: A](f: Throwable ⇒ DataPipeline[B, F, T]): DataPipeline[B, F, T]
  /**
    * Forces evaluation of [[DataPipeline]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/

  def zip[B](that: DataPipeline[B, F, T]): DataPipeline[(A, B), F, T] = ???

  protected[trembita] def evalFunc[B >: A](implicit ev: T <:< PipelineType.Finite): F[Vector[B]]

  protected[trembita] def bindFunc[B >: A](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit]
  //
  //  /**
  //    * Orders elements of the [[DataPipeline]]
  //    * having an [[Ordering]] defined for type [[A]]
  //    *
  //    * @return - the same pipeline sorted
  //    **/
  //  def sorted[B >: A : Ordering : ClassTag](implicit ev: T <:< PipelineType.Finite, F: Functor[F]): DataPipeline[B, F, T]
  //
  //  /**
  //    * Special case of [[distinctBy]]
  //    * Guarantees that each element of pipeline is unique
  //    *
  //    * CONTRACT: the caller is responsible for correct {{{equals}}}
  //    * implemented for type [[A]]
  //    *
  //    * @return - pipeline with only unique elements
  //    **/
  //  def distinct: DataPipeline[A, F, T]
  //
  //  /**
  //    * Guarantees that each element of pipeline is unique
  //    * according to the given criteria
  //    *
  //    * CONTRACT: the caller is responsible for correct {{{equals}}}
  //    * implemented for type [[B]]
  //    *
  //    * @tparam B - uniqueness criteria type
  //    * @param f - function to extract [[B]] from the pipeline element
  //    * @return - pipeline with only unique elements
  //    **/
  //  def distinctBy[B](f: A ⇒ B): DataPipeline[A, F, T] = this.groupBy(f).map { case (_, group) ⇒ group.head }
  //
  //  /**
  //    * Returns a parallel version of this pipeline
  //    *
  //    * Each next transformation on the [[DataPipeline]]
  //    * will be performed in parallel using given [[ExecutionContext]]
  //    *
  //    * @param ec - an execution context
  //    * @return - A [[ParDataPipeline]] with the same elements
  //    **/
  //  def par(implicit ec: ExecutionContext): ParDataPipeline[A, F, T]
  //
  //  /**
  //    * Returns a sequential version of this pipeline
  //    *
  //    * Each next transformation on the [[DataPipeline]]
  //    * will be performed sequentially
  //    *
  //    * @return - sequential pipeline with the same elements
  //    **/
  //  def seq: DataPipeline[A, F, T]
  //
  //  /**
  //    * Groups the pipeline using given grouping criteria.
  //    *
  //    * Returns a [[GroupByPipeline]] - special implementation of [[DataPipeline]]
  //    *
  //    * @tparam K - grouping criteria
  //    * @param f - function to extract [[K]] from [[A]]
  //    * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
  //    **/
  //  def groupBy[K](f: A ⇒ K): DataPipeline[(K, Iterable[A]), F, T] = new GroupByPipeline[K, A](f, this)
  //
  //
  //  /**
  //    * Pushes the given element into pipeline
  //    *
  //    * @param elem - element to push
  //    * @return - pipeline containing the given element
  //    **/
  //  def :+[B >: A](elem: B): DataPipeline[B, F, T]
  //
  //  /**
  //    * Concatenates the pipeline with the second one
  //    *
  //    * @param that - some other pipeline
  //    * @return - this and that pipelines contatenated
  //    **/
  //  def ++[B >: A, T1 >: T <: PipelineType, T2 <: PipelineType]
  //  (that: DataPipeline[B, F, T2])(implicit concat: PipelineType.Concat[T1, T2])
  //  : DataPipeline[B, F, concat.Out]
}

object DataPipeline {
  /**
    * Wraps given elements into a [[DataPipeline]]
    *
    * @param xs - elements to wrap
    * @return - a [[StrictSource]]
    **/
  def apply[A](xs: A*): DataPipeline[A, Try, PipelineType.Finite] =
    new StrictSource[A, Try, PipelineType.Finite](Success(xs.toIterator))

  def applyF[A, F[_]](xs: A*)(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, PipelineType.Finite] =
    new StrictSource[A, F, PipelineType.Finite](xs.toIterator.pure[F])

  def infinite[A, F[_]](f: ⇒ A)(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, PipelineType.Infinite] =
    new StrictSource[A, F, PipelineType.Infinite](Iterator.continually(f).pure[F])

  def infiniteF[A, F[_]](f: ⇒ F[A])(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, PipelineType.Infinite] =
    new StrictSource[F[A], F, PipelineType.Infinite](Iterator.continually(f).pure[F]).mapM(identity)
  /**
    * Wraps an [[Iterable]] passed by-name
    *
    * @param it - an iterable haven't been evaluated yet
    * @return - a [[StrictSource]]
    **/
  def from[A](it: ⇒ Iterable[A]): DataPipeline[A, Try, PipelineType.Finite] =
    new StrictSource[A, Try, PipelineType.Finite](Try { it.toIterator })

  def fromEffect[A, F[_], T <: PipelineType](fa: F[Iterable[A]])(implicit F: MonadError[F, Throwable]): DataPipeline[A, F, T] =
    new StrictSource[A, F, T](fa.map(_.toIterator))
  /**
    * @return - an empty [[DataPipeline]]
    **/
  def empty[A]: DataPipeline[A, Try, PipelineType.Finite] = new StrictSource[A, Try, PipelineType.Finite](Success(Iterator.empty))

  /**
    * Creates a [[DataPipeline]]
    * containing the result of repeatable call of the given function
    *
    * @param times - size of the resulting pipeline
    * @param fa    - factory function
    * @return - data pipeline
    **/
  def repeat[A](times: Int)(fa: ⇒ A): DataPipeline[A, Try, PipelineType.Finite] =
    new StrictSource(Success((1 to times).toIterator.map(_ ⇒ fa)))

  /**
    * Creates a [[DataPipeline]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts(size: Int): DataPipeline[Int, Try, PipelineType.Finite] = repeat(size)(Random.nextInt())

  /**
    * Creates a [[DataPipeline]]
    * of pseudo-random integers
    *
    * @param size - size of the resulting pipeline
    * @param max  - upper limit for resulting integers
    * @return - pipeline consisting of pseudo-random numbers
    **/
  def randomInts(size: Int, max: Int): DataPipeline[Int, Try, PipelineType.Finite] = repeat(size)(Random.nextInt(max))

  /**
    * Creates a [[DataPipeline]]
    * from lines of the given files
    *
    * @param fileName - file name
    * @return - pipeline with file lines as elements
    **/
  def fromFile(fileName: String): DataPipeline[String, Try, PipelineType.Finite] = new StrictSource(
    Success(scala.io.Source.fromFile(fileName).getLines())
  )
}