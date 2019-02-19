package trembita.internal

import scala.language.higherKinds
import cats._
import cats.data.Nested
import cats.effect._
import cats.implicits._
import trembita._

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import java.util.concurrent.atomic.AtomicReference

import trembita.operations.{CanFlatMap, CanSort, InjectTaggedK}

import scala.util.Try

/**
  * A [[BiDataPipelineT]]
  * that was mapped
  *
  * @tparam Er - error type
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita] class MappingPipelineT[F[_], Er, +A, B, Ex <: Environment](
    f: A => B,
    source: BiDataPipelineT[F, Er, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MappingPipelineT[F, Er, A, C, Ex](f2.compose(f), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, A, C, Ex](a => f2(f(a)), source)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, Ex] =
    new HandleErrorPipelineT[F, Err, A, BB, Ex](f.withErrorCatched[F, Err].asInstanceOf[A => F[BB]], f2, source)(F)

  def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, Ex] =
    new HandleErrorWithPipelineT[F, Err, A, C, Ex](f.withErrorCatched[F, Err].asInstanceOf[A => F[C]], fallback, source)(F)

  def collectImpl[C: ClassTag](pf: PartialFunction[B, C])(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er, C, Ex] = new CollectPipelineT[F, Er, B, C, Ex](pf, this)(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex))(
      vs => Ex.FlatMapRepr.map(vs)(f).asInstanceOf[Ex.Repr[C]]
    )
}

/**
  * A [[BiDataPipelineT]]
  * that was map-concatenated
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita] class MapConcatPipelineT[F[_], Er, +A, B, Ex <: Environment](
    f: A => Iterable[B],
    source: BiDataPipelineT[F, Er, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, A, C, Ex](f(_).map(f2), source)(F)

  /** Each next flatMap will compose [[f]] with some other map function */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, A, C, Ex](f(_).flatMap(f2), source)(F)

  /** Filters the result of [[f]] application */
  override def filterImpl[BB >: B](
      p: B => Boolean
  )(implicit F: Monad[F], B: ClassTag[BB]): BiDataPipelineT[F, Er, BB, Ex] =
    new MapConcatPipelineT[F, Er, A, BB, Ex](f(_).filter(p), source)(F)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, A, C, Ex](f(_).collect(pf), source)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, Ex] =
    new HandleErrorPipelineT[F, Err, A, Iterable[BB], Ex](
      f.withErrorCatched[F, Err].asInstanceOf[A => F[Iterable[BB]]],
      fallback = e => List(f2(e)),
      source
    )(F).mapConcatImpl(identity _)

  def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, Ex] = this // todo: think about it

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex)) { repr =>
      Ex.FlatMapRepr.mapConcat(repr)(f).asInstanceOf[Ex.Repr[C]]
    }
}

protected[trembita] class CollectPipelineT[F[_], Er, +A, B, Ex <: Environment](
    pf: PartialFunction[A, B],
    source: BiDataPipelineT[F, Er, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, Ex] {
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new CollectPipelineT[F, Er, A, C, Ex](pf.andThen(f2), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, B, C, Ex](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
      pf2: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new CollectPipelineT[F, Er, A, C, Ex](pf.andThen(pf2), source)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      fallback: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, Ex] =
    new MapConcatPipelineT[F, Err, A, B, Ex](pf.lift(_).toIterable, source)(F).handleErrorImpl[Err, BB](fallback)(implicitly, F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, Ex] =
    new MapConcatPipelineT[F, Err, A, B, Ex](pf.lift(_).toIterable, source)(F).handleErrorWithImpl[Err, C](fallback)(implicitly, F)

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex))(Ex.collect(_)(pf).asInstanceOf[Ex.Repr[C]])
}

protected[trembita] class HandleErrorPipelineT[F[_], Er, +A, B, Ex <: Environment](
    f: A => F[B],
    fallback: Er => B,
    source: BiDataPipelineT[F, Er, A, Ex]
)(F: MonadError[F, Er])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, Ex] {

  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MappingPipelineT[F, Er, B, C, Ex](f2, this)(F)

  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, B, C, Ex](f2, this)(F)

  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new CollectPipelineT[F, Er, B, C, Ex](pf, this)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, Ex] =
    new HandleErrorPipelineT[F, Err, A, BB, Ex]({ a =>
      F.handleError[BB](f(a).asInstanceOf[F[BB]])(f2)
    }, f2, source)(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, Ex] =
    new HandleErrorWithPipelineT[F, Err, A, C, Ex](
      f.asInstanceOf[A => F[C]],
      e => F.handleErrorWith(F.pure(fallback(e.asInstanceOf[Er]): C))(fallback2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(
      source
        .evalFunc[A](Ex)
    )(
      repr =>
        Ex.TraverseRepr
          .sequence[F, C](
            Ex.FlatMapRepr
              .map[A, F[C]](repr) { a =>
                F.handleError(f(a))(fallback).asInstanceOf[F[C]]
              }(ClassTag(F.unit.getClass).asInstanceOf[ClassTag[F[C]]])
          )(run, B.asInstanceOf[ClassTag[C]])
    )
}

protected[trembita] class HandleErrorWithPipelineT[F[_], Er, +A, B, Ex <: Environment](
    f: A => F[B],
    fallback: Er => F[B],
    source: BiDataPipelineT[F, Er, A, Ex]
)(F: MonadError[F, Er])(implicit B: ClassTag[B])
    extends SeqSource[F, Er, B, Ex](F) {

  override def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, Ex] =
    new HandleErrorWithPipelineT[F, Err, A, BB, Ex](
      f.asInstanceOf[A => F[BB]],
      e => fallback(e.asInstanceOf[Er]).asInstanceOf[F[BB]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      f2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, Ex] =
    new HandleErrorWithPipelineT[F, Err, A, C, Ex](
      f.asInstanceOf[A => F[C]],
      e => fallback(e.asInstanceOf[Er]).asInstanceOf[F[C]].handleErrorWith(f2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(source.evalFunc[A](Ex))(
      vs =>
        Ex.TraverseRepr
          .traverse(vs) { a =>
            F.handleErrorWith(f(a))(fallback).asInstanceOf[F[C]]
          }(B.asInstanceOf[ClassTag[C]], run)
    )
}

protected[trembita] class MapMonadicPipelineT[
    F[_],
    Er,
    +A,
    B,
    Ex <: Environment
](f: A => F[B], source: BiDataPipelineT[F, Er, A, Ex])(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MappingPipelineT[F, Er, B, C, Ex](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new MapConcatPipelineT[F, Er, B, C, Ex](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, Ex] =
    new CollectPipelineT[F, Er, B, C, Ex](pf, this)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, Ex] =
    new MapMonadicPipelineT[F, Err, A, BB, Ex](
      a => f(a).asInstanceOf[F[BB]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      f2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, Ex] =
    new MapMonadicPipelineT[F, Err, A, C, Ex](
      a => f(a).asInstanceOf[F[C]].handleErrorWith(f2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.flatMap(source.evalFunc[A](Ex)) { vs =>
      val resultF = Ex.TraverseRepr.traverse(vs)(f)
      resultF.asInstanceOf[F[Ex.Repr[C]]]
    }
}

object BridgePipelineT {
  protected[trembita] def make[F[_], Er, A, Ex0 <: Environment, Ex1 <: Environment](
      source: BiDataPipelineT[F, Er, A, Ex0],
      Ex0: Ex0,
      F: Monad[F]
  )(implicit A: ClassTag[A],
    run0: Ex0.Run[F],
    @transient inject: InjectTaggedK[Ex0.Repr, λ[β => F[Ex1#Repr[β]]]]): BiDataPipelineT[F, Er, A, Ex1] =
    new SeqSource[F, Er, A, Ex1](F) {
      override def handleErrorImpl[Err >: Er, B >: A: ClassTag](
          f: Err => B
      )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Ex1] =
        make[F, Err, B, Ex0, Ex1](source.handleErrorImpl[Err, B](f), Ex0, F)(
          implicitly[ClassTag[B]],
          run0,
          inject
        )

      override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](
          f: Err => F[B]
      )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Ex1] =
        make[F, Err, B, Ex0, Ex1](source.handleErrorWithImpl[Err, B](f), Ex0, F)(
          implicitly[ClassTag[B]],
          run0,
          inject
        )

      protected[trembita] def evalFunc[B >: A](
          Ex: Ex1
      )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
        F.flatMap(
          source
            .evalFunc[A](Ex0)
        )(vs => inject(vs).asInstanceOf[F[Ex.Repr[B]]])
    }
}

/**
  * [[BiDataPipelineT]] subclass
  * with basic operations implemented:
  *
  * [[BiDataPipelineT.mapImpl]]      ~> [[MappingPipelineT]]
  * [[BiDataPipelineT.mapConcatImpl]]  ~> [[MapConcatPipelineT]]
  **/
protected[trembita] abstract class SeqSource[F[_], Er, +A, Ex <: Environment](
    F: Monad[F]
)(implicit A: ClassTag[A])
    extends BiDataPipelineT[F, Er, A, Ex] {
  def mapImpl[B: ClassTag](
      f: A => B
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, B, Ex] =
    new MappingPipelineT[F, Er, A, B, Ex](f, this)(F)

  def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, B, Ex] =
    new MapConcatPipelineT[F, Er, A, B, Ex](f, this)(F)

  def collectImpl[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, B, Ex] =
    new CollectPipelineT[F, Er, A, B, Ex](pf, this)(F)

  def handleErrorImpl[Err >: Er, B >: A: ClassTag](f: Err => B)(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, Ex] = this

  override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => F[B])(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, Ex] = this
}

/**
  * Concrete implementation of [[BiDataPipelineT]]
  * wrapping by-name [[Iterable]]
  *
  * @tparam A - type of pipeline elements
  * @param iterF - not evaluated yet collection of [[A]]
  **/
protected[trembita] class StrictSource[F[_], Er, +A](
    iterF: => F[Iterator[A]],
    F: Monad[F]
)(implicit A: ClassTag[A], Fctg: ClassTag[F[_]])
    extends SeqSource[F, Er, A, Sequential](F) {
  override def handleErrorImpl[Err >: Er, B >: A: ClassTag](
      f: Err => B
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Sequential] =
    new StrictSource[F, Err, F[B]](
      iterF.map { iterator =>
        new Iterator[F[B]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[B] =
            ((_: Unit) => iterator.next(): B).withErrorCatched[F, Err].apply(()).handleError(f)
        }
      },
      F
    )(ClassTag[F[B]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)

  override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](
      f: Err => F[B]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Sequential] =
    new StrictSource[F, Err, F[B]](
      iterF.map { iterator =>
        new Iterator[F[B]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[B] =
            ((_: Unit) => iterator.next(): B).withErrorCatched[F, Err].apply(()).handleErrorWith(f)
        }
      },
      F
    )(ClassTag[F[B]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)

  /**
    * Forces evaluation of [[BiDataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](
      Ex: Sequential
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(iterF)(iter => iter.toVector.asInstanceOf[Ex.Repr[B]])
}

object MapKPipelineT {
  def make[F[_], Er, G[_], A, Ex <: Environment](source: BiDataPipelineT[F, Er, A, Ex], ex0: Ex, arrow: F ~> G, G: Monad[G])(
      implicit A: ClassTag[A],
      run0: ex0.Run[F]
  ): BiDataPipelineT[G, Er, A, Ex] =
    new SeqSource[G, Er, A, Ex](G) {
      protected[trembita] def evalFunc[B >: A](
          Ex: Ex
      )(implicit run: Ex.Run[G]): G[Ex.Repr[B]] =
        arrow(source.evalFunc[B](ex0)).asInstanceOf[G[Ex.Repr[B]]]
    }
}

/**
  * A [[BiDataPipelineT]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
protected[trembita] class SortedPipelineT[+A: Ordering, F[_], Er, Ex <: Environment](
    source: BiDataPipelineT[F, Er, A, Ex],
    F: Monad[F],
    canSort: CanSort[Ex#Repr]
)(implicit A: ClassTag[A])
    extends SeqSource[F, Er, A, Ex](F) {
  override def handleErrorImpl[Err >: Er, B >: A: ClassTag](
      f: Err => B
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Ex] =
    new SortedPipelineT[B, F, Err, Ex](
      source.handleErrorImpl[Err, B](f),
      F,
      canSort
    )(Ordering[A].asInstanceOf[Ordering[B]], A.asInstanceOf[ClassTag[B]])

  protected[trembita] def evalFunc[B >: A](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(source.evalFunc[A](Ex)) { vs =>
      canSort.sorted(vs).asInstanceOf[Ex.Repr[B]]
    }
}

object EvaluatedSource {
  def make[F[_], Er, A: ClassTag, E <: Environment](
      reprF: F[E#Repr[A]],
      F: Monad[F]
  ): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      override def evalFunc[B >: A](
          Ex: E
      )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
        reprF.asInstanceOf[F[Ex.Repr[B]]]
    }
}

object MapReprPipeline {
  def make[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      e: E
  )(f: e.Repr[A] => e.Repr[B], F: Monad[F], run: e.Run[F]): BiDataPipelineT[F, Er, B, E] =
    new SeqSource[F, Er, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[C]] =
        F.map(source.evalFunc[A](e)(run))(f).asInstanceOf[F[Ex.Repr[C]]]
    }
}

object MapReprFPipeline {
  def make[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      e: E
  )(f: e.Repr[A] => F[e.Repr[B]], F: Monad[F], run: e.Run[F])(implicit canFlatMap: CanFlatMap[E]): BiDataPipelineT[F, Er, B, E] =
    new SeqSource[F, Er, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[C]] =
        F.flatMap(source.evalFunc[A](e)(run)) { reprA =>
          f(reprA).asInstanceOf[F[Ex.Repr[C]]]
        }
    }
}
