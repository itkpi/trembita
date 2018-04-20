package com.datarootlabs.trembita.internal


import scala.language.higherKinds
import cats._
import cats.data.Nested
import cats.effect._
import cats.implicits._
import com.datarootlabs.trembita._
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
class MappingPipeline[+A, B, F[_], T <: Finiteness, Ex <: Execution]
(f: A => B, source: DataPipeline[A, F, T, Ex])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T, Ex] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C, F, T, Ex] = new MappingPipeline[A, C, F, T, Ex](f2.compose(f), source)

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex](a ⇒ f2(f(a)), source)

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): DataPipeline[B, F, T, Ex] = new FlatMapPipeline[A, B, F, T, Ex](
    a => new StrictSource(Some(f(a)).filter(p).toIterator.pure[F]),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T, Ex] = new FlatMapPipeline[A, C, F, T, Ex](
    a => new StrictSource(Some(f(a)).collect(pf).toIterator.pure[F]),
    source
  )

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T, Ex] = new MapMonadicPipeline[A, C, F, T, Ex](f2.compose(f), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T, Ex] =
    new MapMonadicPipeline[A, C, F, T, Ex](a ⇒ funcK(f2(f(a))), source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T, Ex] = new HandleErrorPipeline[A, BB, F, T, Ex](f, f2, source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex]({ a ⇒
      try {
        val b = List(f(a))
        new StrictSource(b.toIterator.pure[F])
      } catch {
        case e: Throwable ⇒ f2(e)
      }
    }, source)

  //  def zip[C](that: DataPipeline[C, F, T, Ex]): DataPipeline[(B, C), F, T, Ex] =
  //    new MappingPipeline[(A, C), (B, C), F, T, Ex]({ case (a, b) ⇒ (f(a), b) }, source.zip(that))

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[C]] =
    source.evalFunc[A](ev, Ex).map(vs ⇒ Ex.Monad.map(vs)(f))

  protected[trembita] def consumeFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.consumeFunc[A] {
      case Right(a) ⇒ f2(try Right(f(a)) catch {
        case e: Throwable ⇒ Left(e)
      })
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
class FlatMapPipeline[+A, B, F[_], T <: Finiteness, Ex <: Execution]
(f: A => DataPipeline[B, F, T, Ex], source: DataPipeline[A, F, T, Ex])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T, Ex] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C, F, T, Ex] = new FlatMapPipeline[A, C, F, T, Ex](f(_).map(f2), source)


  /** Each next flatMap will compose [[f]] with some other map function */
  def flatMap[C](f2: B => DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex](f(_).flatMap(f2), source)


  /** Filters the result of [[f]] application */
  def filter(p: B => Boolean): FlatMapPipeline[A, B, F, T, Ex] = new FlatMapPipeline[A, B, F, T, Ex](f(_).filter(p), source)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex](f(_).collect(pf), source)

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T, Ex] = new FlatMapPipeline[A, C, F, T, Ex](f(_).mapM(f2), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex](f(_).mapK(f2), source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T, Ex] = new FlatMapPipeline[A, BB, F, T, Ex]({ a ⇒
    f(a).handleError(f2)
  }, source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex]({ a ⇒
      try f(a) catch {
        case e: Throwable ⇒ f2(e)
      }
    }, source)

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[C]] =
    source.evalFunc[A](ev, Ex).flatMap { vs ⇒
      val evaluated = Ex.Monad.map(vs)(f(_).evalFunc[B](ev, Ex))

      val res = Ex.Traverse.sequence[F, Ex.Repr[B]](evaluated).map(Ex.Monad.flatten(_))

      res.asInstanceOf[F[Ex.Repr[C]]]
    }

  protected[trembita] def consumeFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.consumeFunc[A] {
      case Right(a) ⇒ f(a).consumeFunc(f2)
      case Left(e)  ⇒ f2(Left(e))
    }
}

class CollectPipeline[+A, B, F[_], T <: Finiteness, Ex <: Execution]
(pf: PartialFunction[A, B], source: DataPipeline[A, F, T, Ex])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T, Ex] {
  def map[C](f2: B => C): DataPipeline[C, F, T, Ex] = new CollectPipeline[A, C, F, T, Ex](pf.andThen(f2), source)

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[B, C, F, T, Ex](f2, this)

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): DataPipeline[B, F, T, Ex] = new FlatMapPipeline[A, B, F, T, Ex](
    a => new StrictSource(List(a).collect(pf).filter(p).toIterator.pure[F]),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf2: PartialFunction[B, C]): DataPipeline[C, F, T, Ex] = new CollectPipeline[A, C, F, T, Ex](
    pf.andThen(pf2),
    source
  )

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex](a ⇒ new StrictSource[C, F, T, Ex]({
      Some(a).collect(pf) match {
        case None    ⇒ F.pure[Iterator[C]](Iterator.empty)
        case Some(b) ⇒ f2(b).map(List(_).toIterator)
      }
    }), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex](a ⇒ new StrictSource[C, F, T, Ex]({
      Some(a).collect(pf) match {
        case None    ⇒ F.pure[Iterator[C]](Iterator.empty)
        case Some(b) ⇒ funcK(f2(b)).map(List(_).toIterator)
      }
    }), source)


  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T, Ex] =
    new CollectPipeline[A, BB, F, T, Ex]({
      case a if pf.isDefinedAt(a) ⇒ try pf(a) catch {
        case e: Throwable ⇒ f2(e)
      }
    }, source)

  def handleErrorWith[C >: B](f: Throwable ⇒ DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex]({ a ⇒
      try {
        val b = List(a).collect(pf)
        new StrictSource(b.toIterator.pure[F])
      } catch {
        case e: Throwable ⇒ f(e)
      }
    }, source)

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[C]] =
    source.evalFunc[A](ev, Ex).map(Ex.collect(_)(pf))

  protected[trembita] def consumeFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.consumeFunc[A]({
      case Right(a) if pf.isDefinedAt(a) ⇒ f2({
        try Right(pf(a)) catch {
          case e: Throwable ⇒ Left(e)
        }
      })
      case Right(_)                      ⇒ F.unit
      case Left(e)                       ⇒ f2(Left(e))
    })
}

protected[trembita]
class HandleErrorPipeline[+A, B, F[_], T <: Finiteness, Ex <: Execution]
(f: A ⇒ B, fallback: Throwable => B, source: DataPipeline[A, F, T, Ex])
(implicit F: MonadError[F, Throwable]
) extends DataPipeline[B, F, T, Ex] {

  def map[C](f2: B ⇒ C): DataPipeline[C, F, T, Ex] = new MappingPipeline[A, C, F, T, Ex]({ a ⇒
    val b = try f(a) catch {
      case e: Throwable ⇒ fallback(e)
    }
    f2(b)
  }, source)

  def flatMap[C](f2: B ⇒ DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[B, C, F, T, Ex](f2, this)

  def filter(p: B ⇒ Boolean): DataPipeline[B, F, T, Ex] = new CollectPipeline[B, B, F, T, Ex]({ case b if p(b) ⇒ b }, this)

  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T, Ex] = new CollectPipeline[B, C, F, T, Ex](pf, this)

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T, Ex] = new MapMonadicPipeline[A, C, F, T, Ex]({ a ⇒
    a.pure[F].map(f).handleError(fallback).flatMap(f2)
  }, source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T, Ex] = new MapMonadicPipeline[A, C, F, T, Ex]({ a ⇒
    a.pure[F].map(f).handleError(fallback).flatMap(b ⇒ funcK(f2(b)))
  }, source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T, Ex] = new HandleErrorPipeline[A, BB, F, T, Ex]({ a ⇒
    try f(a) catch {
      case e: Throwable ⇒ fallback(e)
    }
  }, f2, source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new FlatMapPipeline[A, C, F, T, Ex]({ a ⇒
      try {
        val b = List(try f(a) catch {
          case e: Throwable ⇒ fallback(e)
        })
        new StrictSource(b.toIterator.pure[F])
      } catch {
        case e: Throwable ⇒ f2(e)
      }
    }, source)

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[C]] =
    source.evalFunc[A](ev, Ex).map(Ex.Monad.map(_) { a ⇒
      try f(a) catch {
        case e: Throwable ⇒ fallback(e)
      }
    })

  protected[trembita] def consumeFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.consumeFunc[A] {
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
class MapMonadicPipeline[+A, B, F[_], T <: Finiteness, Ex <: Execution]
(f: A => F[B], source: DataPipeline[A, F, T, Ex])(implicit ME: MonadError[F, Throwable]) extends DataPipeline[B, F, T, Ex] {
  /** Each next map will compose [[f]] with some other map function */
  def map[C](f2: B => C): DataPipeline[C, F, T, Ex] = new MapMonadicPipeline[A, C, F, T, Ex](
    a => f(a).map(f2), source
  )

  /** Returns [[FlatMapPipeline]] */
  def flatMap[C](f2: B => DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] = new FlatMapPipeline[B, C, F, T, Ex](f2, this)

  /** Returns [[FlatMapPipeline]] with filter function applied */
  def filter(p: B => Boolean): DataPipeline[B, F, T, Ex] = new FlatMapPipeline[A, B, F, T, Ex](
    a => new StrictSource(f(a).map { b ⇒ List(b).filter(p).toIterator }),
    source
  )

  /** Returns [[FlatMapPipeline]] with [[PartialFunction]] applied */
  def collect[C](pf: PartialFunction[B, C]): DataPipeline[C, F, T, Ex] = new FlatMapPipeline[A, C, F, T, Ex](
    a => new StrictSource(f(a).map { b ⇒ List(b).collect(pf).toIterator }),
    source
  )

  def mapM[C](f2: B ⇒ F[C]): DataPipeline[C, F, T, Ex] =
    new MapMonadicPipeline[A, C, F, T, Ex](a ⇒ f(a).flatMap(f2), source)

  def mapK[C, G[_]](f2: B ⇒ G[C])(implicit funcK: G ~> F): DataPipeline[C, F, T, Ex] =
    new MapMonadicPipeline[A, C, F, T, Ex](a ⇒ f(a).flatMap(b ⇒ funcK(f2(b))), source)

  def handleError[BB >: B](f2: Throwable ⇒ BB): DataPipeline[BB, F, T, Ex] =
    new MapMonadicPipeline[A, BB, F, T, Ex](a ⇒ f(a).asInstanceOf[F[BB]].handleError(f2), source)

  def handleErrorWith[C >: B](f2: Throwable ⇒ DataPipeline[C, F, T, Ex]): DataPipeline[C, F, T, Ex] =
    new MapMonadicPipeline[A, DataPipeline[C, F, T, Ex], F, T, Ex]({ a ⇒
      f(a).map { b ⇒ new StrictSource(List(b).toIterator.pure[F]): DataPipeline[C, F, T, Ex] }
        .handleError(f2)
    }, source).flatten

  protected[trembita] def evalFunc[C >: B](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[C]] =
    source.evalFunc[A](ev, Ex).flatMap { vs ⇒
      val resultF: F[Ex.Repr[C]] = Ex.Traverse.sequence[F, C](Ex.Monad.map(vs) { a ⇒ f(a).asInstanceOf[F[C]] })
      resultF
    }

  protected[trembita] def consumeFunc[C >: B](f2: Either[Throwable, C] ⇒ F[Unit]): F[Unit] =
    source.consumeFunc[A] {
      case Right(a) ⇒ f(a).map(Right(_): Either[Throwable, C]).handleError { e ⇒ Left(e) }.flatMap(f2)
      case Left(e)  ⇒ f2(Left(e))
    }
}

protected[trembita]
class BridgePipeline[+A, F[_], T <: Finiteness, Ex1 <: Execution, Ex2 <: Execution]
(source: DataPipeline[A, F, T, Ex1], ex2: Ex2)
(implicit F: MonadError[F, Throwable], Ex1: Ex1) extends SeqSource[A, F, T, Ex2] {
  def handleError[B >: A](f: Throwable => B): DataPipeline[B, F, T, Ex2] = new BridgePipeline(source.handleError(f), ex2)

  def handleErrorWith[B >: A](f: Throwable => DataPipeline[B, F, T, Ex2]): DataPipeline[B, F, T, Ex2] =
    new BridgePipeline(
      source.handleErrorWith(e => new BridgePipeline[B, F, T, Ex2, Ex1](f(e), Ex1)(F, ex2)),
      ex2
    )

  protected[trembita] def evalFunc[B >: A](implicit ev: <:<[T, Finiteness.Finite], Ex: Ex2): F[Ex.Repr[B]] =
    source.evalFunc[A](ev, Ex1).map(vs => Ex.fromVector(Ex1.toVector(vs.asInstanceOf[Ex1.Repr[A]])))

  protected[trembita] def consumeFunc[B >: A](f: Either[Throwable, B] => F[Unit]): F[Unit] = source.consumeFunc(f)
}

/**
  * [[DataPipeline]] subclass
  * with basic operations implemented:
  *
  * [[DataPipeline.map]]      ~> [[MappingPipeline]]
  * [[DataPipeline.flatMap]]  ~> [[FlatMapPipeline]]
  **/
protected[trembita]
abstract class SeqSource[+A, F[_], T <: Finiteness, Ex <: Execution](implicit F: MonadError[F, Throwable]) extends DataPipeline[A, F, T, Ex] {
  def map[B](f: A => B): DataPipeline[B, F, T, Ex] = new MappingPipeline[A, B, F, T, Ex](f, this)

  def flatMap[B](f: A => DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex] = new FlatMapPipeline[A, B, F, T, Ex](f, this)

  def filter(p: A => Boolean): DataPipeline[A, F, T, Ex] = new CollectPipeline[A, A, F, T, Ex](
    { case a if p(a) ⇒ a }, this
  )

  def collect[B](pf: PartialFunction[A, B]): DataPipeline[B, F, T, Ex] = new CollectPipeline[A, B, F, T, Ex](pf, this)

  def mapM[B](f: A ⇒ F[B]): DataPipeline[B, F, T, Ex] = new MapMonadicPipeline[A, B, F, T, Ex](f, this)

  def mapK[B, G[_]](f2: A ⇒ G[B])(implicit funcK: G ~> F): DataPipeline[B, F, T, Ex] =
    new MapMonadicPipeline[A, B, F, T, Ex](a ⇒ funcK(f2(a)), this)
}

/**
  * Concrete implementation of [[DataPipeline]]
  * wrapping by-name [[Iterable]]
  *
  * @tparam A - type of pipeline elements
  * @param iterF - not evaluated yet collection of [[A]]
  **/
protected[trembita]
class StrictSource[+A, F[_], T <: Finiteness, Ex <: Execution]
(iterF: => F[Iterator[A]])(implicit F: MonadError[F, Throwable]) extends SeqSource[A, F, T, Ex] {
  def handleError[B >: A](f: Throwable ⇒ B): DataPipeline[B, F, T, Ex] =
    new StrictSource[B, F, T, Ex](iterF.map { iterator ⇒
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

  def handleErrorWith[B >: A](f: Throwable ⇒ DataPipeline[B, F, T, Ex]): DataPipeline[B, F, T, Ex] =
    new StrictSource[DataPipeline[B, F, T, Ex], F, T, Ex](iterF.map { iterator ⇒
      new Iterator[DataPipeline[B, F, T, Ex]] {
        private var failed: Boolean = false

        def hasNext: Boolean = !failed && iterator.hasNext

        def next(): DataPipeline[B, F, T, Ex] = try {
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
  protected[trembita] def evalFunc[B >: A](implicit ev: T <:< Finiteness.Finite, Ex: Ex): F[Ex.Repr[B]] =
    iterF.map(iter => Ex.fromVector(iter.toVector))

  protected[trembita] def consumeFunc[B >: A](f: Either[Throwable, B] ⇒ F[Unit]): F[Unit] =
    iterF.map(Right(_): Either[Throwable, Iterator[B]])
      .handleError(e ⇒ Left(e))
      .map {
        case Left(e)     ⇒ f(Left(e))
        case Right(iter) ⇒
          if (!iter.hasNext) F.unit
          else {
            F.tailRecM[Iterator[B], Unit](iter) { it ⇒
              if (it.hasNext) {
                val next: Either[Throwable, B] = Try {
                  it.next
                }.toEither
                f(next).map(_ ⇒ Left(it))
              } else F.pure(Right({}))
            }
          }
      }
}

protected[trembita]
class MemoizedPipeline[+A, F[_], Ex <: Execution]
(vsF: F[Vector[A]])(implicit F: MonadError[F, Throwable]) extends SeqSource[A, F, Finiteness.Finite, Ex] {
  def handleError[B >: A](f: Throwable => B): DataPipeline[B, F, Finiteness.Finite, Ex] = this

  def handleErrorWith[B >: A](f: Throwable => DataPipeline[B, F, Finiteness.Finite, Ex]): DataPipeline[B, F, Finiteness.Finite, Ex] = this

  protected[trembita] def evalFunc[B >: A](implicit ev: <:<[Finiteness.Finite, Finiteness.Finite], Ex: Ex): F[Ex.Repr[B]] =
    vsF.map(Ex.fromVector(_))

  protected[trembita] def consumeFunc[B >: A](f: Either[Throwable, B] => F[Unit]): F[Unit] = vsF.map(vs =>
    Traverse[Vector].traverse[F, B, Unit](vs)(b => f(Right(b)))
  )
}

/**
  * A [[DataPipeline]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
protected[trembita]
class SortedPipeline[+A: Ordering, F[_], Ex <: Execution](source: DataPipeline[A, F, Finiteness.Finite, Ex], ex: Ex)(implicit me: MonadError[F, Throwable]) extends SeqSource[A, F, Finiteness.Finite, Ex] {
  def handleError[B >: A](f: Throwable => B): DataPipeline[B, F, Finiteness.Finite, Ex] =
    new SortedPipeline[A, F, Ex](source.handleError(f).asInstanceOf[DataPipeline[A, F, Finiteness.Finite, Ex]], ex)

  def handleErrorWith[B >: A](f: Throwable => DataPipeline[B, F, Finiteness.Finite, Ex]): DataPipeline[B, F, Finiteness.Finite, Ex] =
    new SortedPipeline[A, F, Ex](source.handleErrorWith(f).asInstanceOf[DataPipeline[A, F, Finiteness.Finite, Ex]], ex)

  protected[trembita] def evalFunc[B >: A](implicit ev: <:<[Finiteness.Finite, Finiteness.Finite], Ex: Ex): F[Ex.Repr[B]] =
    source.evalFunc[A](ev, Ex).map[Ex.Repr[B]] { vs =>
      Ex.sorted(vs).asInstanceOf[Ex.Repr[B]]
    }

  protected[trembita] def consumeFunc[B >: A](f: Either[Throwable, B] => F[Unit]): F[Unit] =
    evalFunc[A]($conforms, ex).map { vs =>
      ex.Traverse.traverse(vs.asInstanceOf[ex.Repr[B]])(b => f(Right(b)))
    }
}
