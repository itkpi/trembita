package com.github.trembita.internal

import scala.language.higherKinds
import cats._
import cats.data.Nested
import cats.effect._
import cats.implicits._
import com.github.trembita._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import java.util.concurrent.atomic.AtomicReference

import scala.util.Try

/**
  * A [[DataPipelineT]]
  * that was mapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita] class MappingPipelineT[F[_], +A, B, Ex <: Execution](
  f: A => B,
  source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
    f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MappingPipelineT[F, A, C, Ex](f2.compose(f), source)(F)

  /** Returns [[FlatMapPipelineT]] */
  def flatMapImpl[C: ClassTag](
    f2: B => DataPipelineT[F, C, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, A, C, Ex](a => f2(f(a)), source)(F)

  def handleErrorImpl[BB >: B: ClassTag](
    f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new HandleErrorPipelineT[F, A, BB, Ex](f, f2, source)(F)

  def handleErrorWithImpl[C >: B: ClassTag](
    fallback: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] =
    new HandleErrorWithPipelineT[F, A, C, Ex](f, fallback, source)(F)

  def collectImpl[C: ClassTag](pf: PartialFunction[B, C])(
    implicit F: Monad[F]
  ): DataPipelineT[F, C, Ex] = new CollectPipelineT[F, B, C, Ex](pf, this)(F)

  protected[trembita] def evalFunc[C >: B](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex))(
      vs => Ex.ApplicativeFlatMap.map(vs)(f).asInstanceOf[Ex.Repr[C]]
    )
}

/**
  * A [[DataPipelineT]]
  * that was flatMapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita] class FlatMapPipelineT[F[_], +A, B, Ex <: Execution](
  f: A => DataPipelineT[F, B, Ex],
  source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
    f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, A, C, Ex](f(_).mapImpl(f2), source)(F)

  /** Each next flatMap will compose [[f]] with some other map function */
  def flatMapImpl[C: ClassTag](
    f2: B => DataPipelineT[F, C, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, A, C, Ex](f(_).flatMapImpl(f2), source)(F)

  /** Filters the result of [[f]] application */
  override def filterImpl[BB >: B](
    p: B => Boolean
  )(implicit F: Monad[F], B: ClassTag[BB]): DataPipelineT[F, BB, Ex] =
    new FlatMapPipelineT[F, A, BB, Ex](f(_).filterImpl[BB](p), source)(F)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collectImpl[C: ClassTag](
    pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, A, C, Ex](f(_).collectImpl(pf), source)(F)

  def handleErrorImpl[BB >: B: ClassTag](
    f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new FlatMapPipelineT[F, A, BB, Ex]({ a =>
      f(a).handleErrorImpl(f2)
    }, source)(F)

  def handleErrorWithImpl[C >: B: ClassTag](
    fallback: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, A, C, Ex]({ a =>
      f(a).handleErrorWithImpl(fallback)
    }, source)(F)

  protected[trembita] def evalFunc[C >: B](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(source.evalFunc[A](Ex)) { vs =>
      val res = Ex.Traverse.traverse(vs)(f(_).evalFunc(Ex))(
        ClassTag(vs.getClass.asInstanceOf[Class[Ex.Repr[B]]]),
        run
      )
      F.map(res)(Ex.ApplicativeFlatMap.flatten(_)).asInstanceOf[F[Ex.Repr[C]]]
    }
}

class CollectPipelineT[F[_], +A, B, Ex <: Execution](
  pf: PartialFunction[A, B],
  source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {
  def mapImpl[C: ClassTag](
    f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new CollectPipelineT[F, A, C, Ex](pf.andThen(f2), source)(F)

  /** Returns [[FlatMapPipelineT]] */
  def flatMapImpl[C: ClassTag](
    f2: B => DataPipelineT[F, C, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, B, C, Ex](f2, this)(F)

  /** Returns [[FlatMapPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
    pf2: PartialFunction[B, C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new CollectPipelineT[F, A, C, Ex](pf.andThen(pf2), source)(F)

  def handleErrorImpl[BB >: B: ClassTag](
    f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new CollectPipelineT[F, A, BB, Ex]({
      case a if pf.isDefinedAt(a) =>
        try pf(a)
        catch {
          case e: Throwable => f2(e)
        }
    }, source)(F)

  override def handleErrorWithImpl[C >: B: ClassTag](
    fallback: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] =
    new HandleErrorWithPipelineT[F, A, C, Ex](
      pf(_): C,
      fallback,
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex))(Ex.collect(_)(pf).asInstanceOf[Ex.Repr[C]])
}

protected[trembita] class HandleErrorPipelineT[F[_], +A, B, Ex <: Execution](
  f: A => B,
  fallback: Throwable => B,
  source: DataPipelineT[F, A, Ex]
)(F: MonadError[F, Throwable])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  def mapImpl[C: ClassTag](
    f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MappingPipelineT[F, B, C, Ex](f2, this)(F)

  def flatMapImpl[C: ClassTag](
    f2: B => DataPipelineT[F, C, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, B, C, Ex](f2, this)(F)

  def collectImpl[C: ClassTag](
    pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new CollectPipelineT[F, B, C, Ex](pf, this)(F)

  def handleErrorImpl[BB >: B: ClassTag](
    f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new HandleErrorPipelineT[F, A, BB, Ex]({ a =>
      try f(a)
      catch {
        case e: Throwable => fallback(e)
      }
    }, f2, source)(F)

  override def handleErrorWithImpl[C >: B: ClassTag](
    fallback2: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] =
    new HandleErrorWithPipelineT[F, A, C, Ex](
      f,
      e =>
        try F.pure(fallback(e): C)
        catch {
          case e2: Throwable => fallback2(e2)
      },
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(
      source
        .evalFunc[A](Ex)
    )(
      Ex.ApplicativeFlatMap
        .map(_) { a =>
          try f(a)
          catch {
            case e: Throwable => fallback(e)
          }
        }
        .asInstanceOf[Ex.Repr[C]]
    )
}

protected[trembita] class HandleErrorWithPipelineT[F[_], +A, B, Ex <: Execution](
  f: A => B,
  fallback: Throwable => F[B],
  source: DataPipelineT[F, A, Ex]
)(F: MonadError[F, Throwable])(implicit B: ClassTag[B])
    extends SeqSource[F, B, Ex](F) {

  override def handleErrorImpl[BB >: B: ClassTag](
    f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new HandleErrorWithPipelineT[F, A, BB, Ex](
      f,
      e => fallback(e).asInstanceOf[F[BB]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[C >: B: ClassTag](
    f2: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] =
    new HandleErrorWithPipelineT[F, A, C, Ex](
      f,
      e => fallback(e).asInstanceOf[F[C]].handleErrorWith(f2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex))(
      vs =>
        Ex.Traverse
          .traverse(vs) { a =>
            try {
              F.pure(f(a))
            } catch {
              case e: Throwable => fallback(e)
            }
          }
          .asInstanceOf[Ex.Repr[C]]
    )
}

protected[trembita] class MapMonadicPipelineT[F[_], +A, B, Ex <: Execution](
  f: A => F[B],
  source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
    f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MappingPipelineT[F, B, C, Ex](f2, this)(F)

  /** Returns [[FlatMapPipelineT]] */
  def flatMapImpl[C: ClassTag](
    f2: B => DataPipelineT[F, C, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new FlatMapPipelineT[F, B, C, Ex](f2, this)(F)

  /** Returns [[FlatMapPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
    pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new CollectPipelineT[F, B, C, Ex](pf, this)(F)

  def handleErrorImpl[BB >: B: ClassTag](
    f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new MapMonadicPipelineT[F, A, BB, Ex](
      a => f(a).asInstanceOf[F[BB]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[C >: B: ClassTag](
    f2: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] =
    new MapMonadicPipelineT[F, A, C, Ex](
      a => f(a).asInstanceOf[F[C]].handleErrorWith(f2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(source.evalFunc[A](Ex)) { vs =>
      val resultF = Ex.Traverse.traverse(vs)(f)
      resultF.asInstanceOf[F[Ex.Repr[C]]]
    }
}

object BridgePipelineT {
  protected[trembita] def make[F[_], A, Ex0 <: Execution, Ex1 <: Execution](
    source: DataPipelineT[F, A, Ex0],
    Ex0: Ex0,
    F: Monad[F]
  )(implicit A: ClassTag[A],
    run0: Ex0.Run[F],
    @transient inject: InjectTaggedK[Ex0.Repr, Ex1#Repr])
    : DataPipelineT[F, A, Ex1] =
    new SeqSource[F, A, Ex1](F) {
      override def handleErrorImpl[B >: A: ClassTag](
        f: Throwable => B
      )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex1] =
        make[F, B, Ex0, Ex1](source.handleErrorImpl(f), Ex0, F)(
          implicitly[ClassTag[B]],
          run0,
          inject
        )

      override def handleErrorWithImpl[B >: A: ClassTag](
        f: Throwable => F[B]
      )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex1] =
        make[F, B, Ex0, Ex1](source.handleErrorWithImpl(f), Ex0, F)(
          implicitly[ClassTag[B]],
          run0,
          inject
        )

      protected[trembita] def evalFunc[B >: A](
        Ex: Ex1
      )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
        F.map(
          source
            .evalFunc[A](Ex0)
        )(vs => inject(vs).asInstanceOf[Ex.Repr[B]])
    }
}

/**
  * [[DataPipelineT]] subclass
  * with basic operations implemented:
  *
  * [[DataPipelineT.mapImpl]]      ~> [[MappingPipelineT]]
  * [[DataPipelineT.flatMapImpl]]  ~> [[FlatMapPipelineT]]
  **/
protected[trembita] abstract class SeqSource[F[_], +A, Ex <: Execution](
  F: Monad[F]
)(implicit A: ClassTag[A])
    extends DataPipelineT[F, A, Ex] {
  def mapImpl[B: ClassTag](
    f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new MappingPipelineT[F, A, B, Ex](f, this)(F)

  def flatMapImpl[B: ClassTag](
    f: A => DataPipelineT[F, B, Ex]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new FlatMapPipelineT[F, A, B, Ex](f, this)(F)

  def collectImpl[B: ClassTag](
    pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new CollectPipelineT[F, A, B, Ex](pf, this)(F)

  def handleErrorImpl[B >: A: ClassTag](f: Throwable => B)(
    implicit F: MonadError[F, Throwable]
  ): DataPipelineT[F, B, Ex] = this

  override def handleErrorWithImpl[B >: A: ClassTag](f: Throwable => F[B])(
    implicit F: MonadError[F, Throwable]
  ): DataPipelineT[F, B, Ex] = this
}

/**
  * Concrete implementation of [[DataPipelineT]]
  * wrapping by-name [[Iterable]]
  *
  * @tparam A - type of pipeline elements
  * @param iterF - not evaluated yet collection of [[A]]
  **/
protected[trembita] class StrictSource[F[_], +A](
  iterF: => F[Iterator[A]],
  F: Monad[F]
)(implicit A: ClassTag[A], Fctg: ClassTag[F[_]])
    extends SeqSource[F, A, Sequential](F) {
  override def handleErrorImpl[B >: A: ClassTag](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Sequential] =
    new StrictSource[F, B](iterF.map { iterator =>
      new Iterator[B] {
        def hasNext: Boolean = iterator.hasNext

        def next(): B =
          try iterator.next()
          catch {
            case e: Throwable => f(e)
          }
      }
    }, F)

  override def handleErrorWithImpl[B >: A: ClassTag](
    f: Throwable => F[B]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Sequential] =
    new StrictSource[F, F[B]](iterF.map { iterator =>
      new Iterator[F[B]] {
        def hasNext: Boolean = iterator.hasNext

        def next(): F[B] =
          try {
            (iterator.next(): B).pure[F]
          } catch {
            case e: Throwable => f(e)
          }
      }
    }, F)(ClassTag[F[B]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)

  /**
    * Forces evaluation of [[DataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](
    Ex: Sequential
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(iterF)(iter => iter.toVector.asInstanceOf[Ex.Repr[B]])
}

protected[trembita] class MemoizedPipelineT[F[_], +A, Ex <: Execution](
  source: DataPipelineT[F, A, Ex],
  F: Monad[F]
)(implicit A: ClassTag[A])
    extends SeqSource[F, A, Ex](F) {
  protected[trembita] def evalFunc[B >: A](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(source.evalFunc[A](Ex))(Ex.memoize(_)).asInstanceOf[F[Ex.Repr[B]]]
}

/**
  * A [[DataPipelineT]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
protected[trembita] class SortedPipelineT[+A: Ordering, F[_], Ex <: Execution](
  source: DataPipelineT[F, A, Ex],
  F: Monad[F]
)(implicit A: ClassTag[A])
    extends SeqSource[F, A, Ex](F) {
  override def handleErrorImpl[B >: A: ClassTag](
    f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new SortedPipelineT[A, F, Ex](
      source
        .handleErrorImpl(f)
        .asInstanceOf[DataPipelineT[F, A, Ex]],
      F
    )

//  override def handleErrorWith[B >: A: ClassTag](
//    f: Throwable => DataPipelineT[F, B, Ex]
//  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
//    new SortedPipelineT[A, F, Ex](
//      source
//        .handleErrorWith(f)
//        .asInstanceOf[DataPipelineT[F, A, Ex]],
//      F
//    )

  protected[trembita] def evalFunc[B >: A](
    Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(source.evalFunc[A](Ex)) { vs =>
      Ex.sorted(vs).asInstanceOf[Ex.Repr[B]]
    }
}
