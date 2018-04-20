package com.datarootlabs.trembita

import scala.language.higherKinds
import cats._
import cats.implicits._
import cats.data.Kleisli
import internal._
import scala.util.{Random, Success, Try}
import scala.collection.parallel.immutable.ParVector


sealed trait Finiteness

object Finiteness {

  sealed trait Finite extends Finiteness

  sealed trait Infinite extends Finiteness

  sealed trait Concat[T1 <: Finiteness, T2 <: Finiteness] {
    type Out <: Finiteness
  }

  object Concat {
  }

  sealed trait Zip[T1 <: Finiteness, T2 <: Finiteness] {
    type Out <: Finiteness
  }

  object Zip {
  }

}

trait Execution {
  type Repr[X]
  val Monad   : Monad[Repr]
  val Traverse: Traverse[Repr]

  def toVector[A](repr: Repr[A]): Vector[A]

  def fromVector[A](vs: Vector[A]): Repr[A]

  def groupBy[A, K](vs: Repr[A])(f: A => K): Map[K, Repr[A]]

  def collect[A, B](repr: Repr[A])(pf: PartialFunction[A, B]): Repr[B]

  def sorted[A: Ordering](repr: Repr[A]): Repr[A]
}

object Execution {

  sealed trait Sequential extends Execution {
    type Repr[X] = Vector[X]

    def toVector[A](repr: Vector[A]): Vector[A] = repr

    def collect[A, B](repr: Vector[A])(pf: PartialFunction[A, B]): Vector[B] = repr.collect(pf)

    def fromVector[A](vs: Vector[A]): Vector[A] = vs

    def groupBy[A, K](vs: Vector[A])(f: A => K): Map[K, Vector[A]] = vs.groupBy(f)

    def sorted[A: Ordering](vs: Vector[A]): Vector[A] = vs.sorted

    val Monad   : Monad[Vector]    = cats.Monad[Vector]
    val Traverse: Traverse[Vector] = cats.Traverse[Vector]
  }

  sealed trait Parallel extends Execution {
    type Repr[X] = ParVector[X]

    def toVector[A](repr: ParVector[A]): Vector[A] = repr.seq

    def fromVector[A](vs: Vector[A]): ParVector[A] = vs.par

    def collect[A, B](repr: ParVector[A])(pf: PartialFunction[A, B]): ParVector[B] = repr.collect(pf)

    def sorted[A: Ordering](vs: ParVector[A]): ParVector[A] = vs.seq.sorted.par

    val Monad: Monad[ParVector] = new Monad[ParVector] {
      def pure[A](a: A): ParVector[A] = ParVector(a)

      def flatMap[A, B](fa: ParVector[A])(f: A => ParVector[B]): ParVector[B] = fa.flatMap(f)

      def tailRecM[A, B](a: A)(f: A => ParVector[Either[A, B]]): ParVector[B] = f(a).flatMap {
        case Left(ax) => tailRecM(ax)(f)
        case Right(b) => ParVector(b)
      }

      override def map[A, B](fa: ParVector[A])(f: A => B): ParVector[B] = fa.map(f)
    }

    def groupBy[A, K](vs: ParVector[A])(f: A => K): Map[K, ParVector[A]] = vs.groupBy(f).seq

    val Traverse: Traverse[ParVector] = new Traverse[ParVector] {
      def traverse[G[_], A, B](fa: ParVector[A])(f: A => G[B])(implicit G: Applicative[G]): G[ParVector[B]] = {
        foldRight[A, G[ParVector[B]]](fa, Always(G.pure(ParVector.empty))) { (a, lgvb) =>
          G.map2Eval(f(a), lgvb)(_ +: _)
        }.value
      }

      def foldLeft[A, B](fa: ParVector[A], b: B)(f: (B, A) => B): B = fa.foldLeft(b)(f)

      def foldRight[A, B](fa: ParVector[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa.foldRight(lb)(f)
    }
  }

  implicit val Parallel  : Parallel   = new Parallel {}
  implicit val Sequential: Sequential = new Sequential {}
}

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

  //  /**
  //    * Applies transformation wrapped into [[Kleisli]]
  //    *
  //    * @tparam C - resulting data type
  //    * @param flow - set of transformation to be applied
  //    * @return - transformed [[DataPipeline]]
  //    *
  //    **/
  //  def transform[B >: A, C](flow: Flow[B, C, F, T, Ex]): DataPipeline[C, F, T, Ex] = flow.run(this)

  def handleError[B >: A](f: Throwable ⇒ B): DataPipeline[B, F, T, Ex]

  def handleErrorWith[B >: A](f: Throwable ⇒ DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex]

  /**
    * Forces evaluation of [[DataPipeline]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/

  protected[trembita] def evalFunc[B >: A](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[B]]

  protected[trembita] def bindFunc[B >: A](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit]
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