package com.datarootlabs.trembita.internal


import scala.language.higherKinds
import cats._
import cats.data.Nested
import cats.effect._
import cats.implicits._
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import java.util.concurrent.atomic.AtomicReference

import scala.util.Try


/**
  * A [[DataPipeline]]
  * that was mapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita]
class MappingPipeline[+A, B, F[_], T <: PipelineType]
(f: A => B, source: DataPipeline[A, F, T])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C, F, T] = new MappingPipeline[A, C, F, T](f2.compose(f), source)

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T](a ⇒ f2(f(a)), source)

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): DataPipeline[B, F, T] = new FlatMapPipeline[A, B, F, T](
    a => new StrictSource(Some(f(a)).filter(p).toIterator.pure[F]),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T] = new FlatMapPipeline[A, C, F, T](
    a => new StrictSource(Some(f(a)).collect(pf).toIterator.pure[F]),
    source
  )

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T] = new MapMonadicPipeline[A, C, F, T](f2.compose(f), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T] =
    new MapMonadicPipeline[A, C, F, T](a ⇒ funcK(f2(f(a))), source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T] = new HandleErrorPipeline[A, BB, F, T](f, f2, source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T]({ a ⇒
      try {
        val b = List(f(a))
        new StrictSource(b.toIterator.pure[F])
      } catch {case e: Throwable ⇒ f2(e)}
    }, source)

  //  def zip[C](that: DataPipeline[C, F, T]): DataPipeline[(B, C), F, T] =
  //    new MappingPipeline[(A, C), (B, C), F, T]({ case (a, b) ⇒ (f(a), b) }, source.zip(that))

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< PipelineType.Finite): F[Vector[C]] =
    source.evalFunc[A].map(vs ⇒ vs.map(f))

  protected[trembita] def bindFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.bindFunc[A] {
      case Right(a) ⇒ f2(try Right(f(a)) catch {case e: Throwable ⇒ Left(e)})
      case Left(e)  ⇒ f2(Left(e))
    }
}

/**
  * A [[DataPipeline]]
  * that was flatMapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita]
class FlatMapPipeline[+A, B, F[_], T <: PipelineType]
(f: A => DataPipeline[B, F, T], source: DataPipeline[A, F, T])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C, F, T] = new FlatMapPipeline[A, C, F, T](f(_).map(f2), source)


  /** Each next flatMap will compose [[f]] with some other map function */
  def flatMap[C](f2: B => DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T](f(_).flatMap(f2), source)


  /** Filters the result of [[f]] application */
  def filter(p: B => Boolean): FlatMapPipeline[A, B, F, T] = new FlatMapPipeline[A, B, F, T](f(_).filter(p), source)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T](f(_).collect(pf), source)

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T] = new FlatMapPipeline[A, C, F, T](f(_).mapM(f2), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T](f(_).mapK(f2), source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T] = new FlatMapPipeline[A, BB, F, T]({ a ⇒
    f(a).handleError(f2)
  }, source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T]({ a ⇒
      try f(a) catch {case e: Throwable ⇒ f2(e)}
    }, source)

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< PipelineType.Finite): F[Vector[C]] =
    source.evalFunc[A].flatMap { vs ⇒
      val evaluated: Vector[F[Vector[B]]] = vs.map { a ⇒ f(a).evalFunc[B] }
      Traverse[Vector].sequence(evaluated).map(_.flatten)
    }

  protected[trembita] def bindFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.bindFunc[A] {
      case Right(a) ⇒ f(a).bindFunc(f2)
      case Left(e)  ⇒ f2(Left(e))
    }
}

class CollectPipeline[+A, B, F[_], T <: PipelineType]
(pf: PartialFunction[A, B], source: DataPipeline[A, F, T])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T] {
  def map[C](f2: B => C): DataPipeline[C, F, T] = new CollectPipeline[A, C, F, T](pf.andThen(f2), source)

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[B, C, F, T](f2, this)

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): DataPipeline[B, F, T] = new FlatMapPipeline[A, B, F, T](
    a => new StrictSource(List(a).collect(pf).filter(p).toIterator.pure[F]),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf2: PartialFunction[B, C]): DataPipeline[C, F, T] = new CollectPipeline[A, C, F, T](
    pf.andThen(pf2),
    source
  )

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T](a ⇒ new StrictSource[C, F, T]({
      Some(a).collect(pf) match {
        case None    ⇒ F.pure[Iterator[C]](Iterator.empty)
        case Some(b) ⇒ f2(b).map(List(_).toIterator)
      }
    }), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T](a ⇒ new StrictSource[C, F, T]({
      Some(a).collect(pf) match {
        case None    ⇒ F.pure[Iterator[C]](Iterator.empty)
        case Some(b) ⇒ funcK(f2(b)).map(List(_).toIterator)
      }
    }), source)


  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T] =
    new CollectPipeline[A, BB, F, T]({
      case a if pf.isDefinedAt(a) ⇒ try pf(a) catch {case e: Throwable ⇒ f2(e)}
    }, source)

  def handleErrorWith[C >: B](f: Throwable ⇒ DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T]({ a ⇒
      try {
        val b = List(a).collect(pf)
        new StrictSource(b.toIterator.pure[F])
      } catch {case e: Throwable ⇒ f(e)}
    }, source)

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< PipelineType.Finite): F[Vector[C]] =
    source.evalFunc[A].map(vs ⇒ vs.collect(pf))

  protected[trembita] def bindFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.bindFunc[A]({
      case Right(a) if pf.isDefinedAt(a) ⇒ f2({ try Right(pf(a)) catch {case e: Throwable ⇒ Left(e)} })
      case Right(_)                      ⇒ F.unit
      case Left(e)                       ⇒ f2(Left(e))
    })
}

protected[trembita]
class HandleErrorPipeline[+A, B, F[_], T <: PipelineType]
(f: A ⇒ B, fallback: Throwable => B, source: DataPipeline[A, F, T])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T] {

  def map[C](f2: B ⇒ C): DataPipeline[C, F, T] = new MappingPipeline[A, C, F, T]({ a ⇒
    val b = try f(a) catch {case e: Throwable ⇒ fallback(e)}
    f2(b)
  }, source)

  def flatMap[C](f2: B ⇒ DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[B, C, F, T](f2, this)

  def filter(p: B ⇒ Boolean): DataPipeline[B, F, T] = new CollectPipeline[B, B, F, T]({ case b if p(b) ⇒ b }, this)

  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T] = new CollectPipeline[B, C, F, T](pf, this)

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T] = new MapMonadicPipeline[A, C, F, T]({ a ⇒
    a.pure[F].map(f).handleError(fallback).flatMap(f2)
  }, source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T] = new MapMonadicPipeline[A, C, F, T]({ a ⇒
    a.pure[F].map(f).handleError(fallback).flatMap(b ⇒ funcK(f2(b)))
  }, source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T] = new HandleErrorPipeline[A, BB, F, T]({ a ⇒
    try f(a) catch {case e: Throwable ⇒ fallback(e)}
  }, f2, source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new FlatMapPipeline[A, C, F, T]({ a ⇒
      try {
        val b = List(try f(a) catch {case e: Throwable ⇒ fallback(e)})
        new StrictSource(b.toIterator.pure[F])
      } catch {case e: Throwable ⇒ f2(e)}
    }, source)

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< PipelineType.Finite): F[Vector[C]] =
    source.evalFunc[A].map(vs ⇒ vs.map { a ⇒ try f(a) catch {case e: Throwable ⇒ fallback(e)} })

  protected[trembita] def bindFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.bindFunc[A] {
      case Right(a) ⇒ f2(try {
        Right(try f(a) catch {
          case e: Throwable ⇒ fallback(e)
        })
      } catch {
        case e: Throwable ⇒ Left(e)
      })
      case Left(e)  ⇒ f2(Left(e))
    }
}

protected[trembita]
class MapMonadicPipeline[+A, B, F[_], T <: PipelineType]
(f: A => F[B], source: DataPipeline[A, F, T])(implicit ME: MonadError[F, Throwable]) extends DataPipeline[B, F, T] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C, F, T] = new MapMonadicPipeline[A, C, F, T](
    a => f(a).map(f2), source
  )

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => DataPipeline[C, F, T]): DataPipeline[C, F, T] = new FlatMapPipeline[B, C, F, T](f2, this)

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): DataPipeline[B, F, T] = new FlatMapPipeline[A, B, F, T](
    a => new StrictSource(f(a).map { b ⇒ List(b).filter(p).toIterator }),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T] = new FlatMapPipeline[A, C, F, T](
    a => new StrictSource(f(a).map { b ⇒ List(b).collect(pf).toIterator }),
    source
  )

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T] =
    new MapMonadicPipeline[A, C, F, T](a ⇒ f(a).flatMap(f2), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T] =
    new MapMonadicPipeline[A, C, F, T](a ⇒ f(a).flatMap(b ⇒ funcK(f2(b))), source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T] =
    new MapMonadicPipeline[A, BB, F, T](a ⇒ f(a).asInstanceOf[F[BB]].handleError(f2), source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T]): DataPipeline[C, F, T] =
    new MapMonadicPipeline[A, DataPipeline[C, F, T], F, T]({ a ⇒
      f(a).map { b ⇒ new StrictSource(List(b).toIterator.pure[F]): DataPipeline[C, F, T] }
        .handleError(f2)
    }, source).flatten

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< PipelineType.Finite): F[Vector[C]] =
    source.evalFunc[A].flatMap { vs ⇒
      val resultF: F[Vector[C]] = Traverse[Vector].sequence(vs.map { a ⇒ f(a).asInstanceOf[F[C]] })
      resultF
    }

  protected[trembita] def bindFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.bindFunc[A] {
      case Right(a) ⇒ f(a).map(Right(_): Either[Throwable, C]).handleError { e ⇒ Left(e) }.flatMap(f2)
      case Left(e)  ⇒ f2(Left(e))
    }
}

/**
  * [[DataPipeline]] subclass
  * with basic operations implemented:
  *
  * [[DataPipeline.map]]      ~> [[MappingPipeline]]
  * [[DataPipeline.flatMap]]  ~> [[FlatMapPipeline]]
  **/
protected[trembita]
abstract class SeqSource[+A, F[_], T <: PipelineType](implicit F: MonadError[F, Throwable]) extends DataPipeline[A, F, T] {
  def map[B](f: A => B): DataPipeline[B, F, T] = new MappingPipeline[A, B, F, T](f, this)
  def flatMap[B](f: A => DataPipeline[B, F, T]): DataPipeline[B, F, T] = new FlatMapPipeline[A, B, F, T](f, this)
  def filter(p: A => Boolean): DataPipeline[A, F, T] = new CollectPipeline[A, A, F, T](
    { case a if p(a) ⇒ a }, this
  )
  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B, F, T] = new CollectPipeline[A, B, F, T](pf, this)
  def mapM[B](f: A ⇒ F[B]): DataPipeline[B, F, T] = new MapMonadicPipeline[A, B, F, T](f, this)
  def mapK[B, G[_]](f2: A ⇒ G[B])(implicit funcK: G ~> F): DataPipeline[B, F, T] =
    new MapMonadicPipeline[A, B, F, T](a ⇒ funcK(f2(a)), this)
}

/**
  * Concrete implementation of [[DataPipeline]]
  * wrapping by-name [[Iterable]]
  *
  * @tparam A - type of pipeline elements
  * @param iterF - not evaluated yet collection of [[A]]
  **/
protected[trembita]
class StrictSource[+A, F[_], T <: PipelineType]
(iterF: => F[Iterator[A]])(implicit F: MonadError[F, Throwable]) extends SeqSource[A, F, T] {
  def handleError[B >: A](f: Throwable ⇒ B): DataPipeline[B, F, T] =
    new StrictSource[B, F, T](iterF.map { iterator ⇒
      new Iterator[B] {
        private var failed: Boolean = false
        def hasNext: Boolean = !failed && iterator.hasNext
        def next(): B = try iterator.next() catch {
          case e: Throwable ⇒
            failed = true
            f(e)
        }
      }
    })

  def handleErrorWith[B >: A](f: Throwable ⇒ DataPipeline[B, F, T]): DataPipeline[B, F, T] =
    new StrictSource[DataPipeline[B, F, T], F, T](iterF.map { iterator ⇒
      new Iterator[DataPipeline[B, F, T]] {
        private var failed: Boolean = false
        def hasNext: Boolean = !failed && iterator.hasNext
        def next(): DataPipeline[B, F, T] = try {
          val res = iterator.next()
          new StrictSource(List(res).toIterator.pure[F])
        } catch {
          case e: Throwable ⇒
            failed = true
            f(e)
        }
      }
    }).flatten
  /**
    * Forces evaluation of [[DataPipeline]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](implicit ev: T <:< PipelineType.Finite): F[Vector[B]] =
    iterF.map(_.toVector)

  protected[trembita] def bindFunc[B >: A](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit] =
    iterF.map(Right(_): Either[Throwable, Iterator[B]])
      .handleError(e ⇒ Left(e))
      .map {
        case Left(e)     ⇒ f(Left(e))
        case Right(iter) ⇒
          if (!iter.hasNext) F.unit
          else {
            F.tailRecM[Iterator[B], Unit](iter) { it ⇒
              if (it.hasNext) {
                val next: Either[Throwable, B] = Try { it.next }.toEither
                f(next).map(_ ⇒ Left(it))
              } else F.pure(Right({}))
            }
          }
      }
}

/**
  * Same to [[StrictSource]]
  * but wrapped values already been evaluated
  *
  * @tparam A - type of pipeline elements
  * @param records -  result of [[DataPipeline]] transformations
  **/
protected[trembita]
class CachedPipeline[+A, F[_]]
(records: Vector[A])(implicit F: MonadError[F, Throwable]
) extends StrictSource[A, F, PipelineType.Finite](records.toIterator.pure[F])

///**
//  * A [[DataPipeline]] been sorted
//  *
//  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
//  * @param source - source pipeline to be sorted
//  **/
//protected[trembita]
//class SortedPipeline[+A: Ordering : ClassTag](source: DataPipeline[A]) extends SeqSource[A] {
//}
