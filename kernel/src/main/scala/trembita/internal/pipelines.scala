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
  * A [[DataPipelineT]]
  * that was mapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita] class MappingPipelineT[F[_], +A, B, Ex <: Environment](
    f: A => B,
    source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MappingPipelineT[F, A, C, Ex](f2.compose(f), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, A, C, Ex](a => f2(f(a)), source)(F)

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
      vs => Ex.FlatMapRepr.map(vs)(f).asInstanceOf[Ex.Repr[C]]
    )
}

/**
  * A [[DataPipelineT]]
  * that was map-concatenated
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
  **/
protected[trembita] class MapConcatPipelineT[F[_], +A, B, Ex <: Environment](
    f: A => Iterable[B],
    source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, A, C, Ex](f(_).map(f2), source)(F)

  /** Each next flatMap will compose [[f]] with some other map function */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, A, C, Ex](f(_).flatMap(f2), source)(F)

  /** Filters the result of [[f]] application */
  override def filterImpl[BB >: B](
      p: B => Boolean
  )(implicit F: Monad[F], B: ClassTag[BB]): DataPipelineT[F, BB, Ex] =
    new MapConcatPipelineT[F, A, BB, Ex](f(_).filter(p), source)(F)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, A, C, Ex](f(_).collect(pf), source)(F)

  def handleErrorImpl[BB >: B: ClassTag](
      f2: Throwable => BB
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, BB, Ex] =
    new HandleErrorPipelineT[F, A, Iterable[BB], Ex](
      f,
      fallback = e => List(f2(e)),
      source
    )(F).mapConcatImpl(identity _)

  def handleErrorWithImpl[C >: B: ClassTag](
      fallback: Throwable => F[C]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, C, Ex] = this // todo: think about it

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex)) { repr =>
      Ex.FlatMapRepr.mapConcat(repr)(f).asInstanceOf[Ex.Repr[C]]
    }
}

class CollectPipelineT[F[_], +A, B, Ex <: Environment](
    pf: PartialFunction[A, B],
    source: DataPipelineT[F, A, Ex]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new CollectPipelineT[F, A, C, Ex](pf.andThen(f2), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, B, C, Ex](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
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
    new HandleErrorWithPipelineT[F, A, C, Ex](pf(_): C, fallback, source)(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[C]] =
    F.map(source.evalFunc[A](Ex))(Ex.collect(_)(pf).asInstanceOf[Ex.Repr[C]])
}

protected[trembita] class HandleErrorPipelineT[F[_], +A, B, Ex <: Environment](
    f: A => B,
    fallback: Throwable => B,
    source: DataPipelineT[F, A, Ex]
)(F: MonadError[F, Throwable])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MappingPipelineT[F, B, C, Ex](f2, this)(F)

  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, B, C, Ex](f2, this)(F)

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
      Ex.FlatMapRepr
        .map(_) { a =>
          try f(a)
          catch {
            case e: Throwable => fallback(e)
          }
        }
        .asInstanceOf[Ex.Repr[C]]
    )
}

protected[trembita] class HandleErrorWithPipelineT[F[_], +A, B, Ex <: Environment](
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
        Ex.TraverseRepr
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

protected[trembita] class MapMonadicPipelineT[
    F[_],
    +A,
    B,
    Ex <: Environment
](f: A => F[B], source: DataPipelineT[F, A, Ex])(F: Monad[F])(implicit B: ClassTag[B])
    extends DataPipelineT[F, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MappingPipelineT[F, B, C, Ex](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): DataPipelineT[F, C, Ex] =
    new MapConcatPipelineT[F, B, C, Ex](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
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
      val resultF = Ex.TraverseRepr.traverse(vs)(f)
      resultF.asInstanceOf[F[Ex.Repr[C]]]
    }
}

object BridgePipelineT {
  protected[trembita] def make[F[_], A, Ex0 <: Environment, Ex1 <: Environment](
      source: DataPipelineT[F, A, Ex0],
      Ex0: Ex0,
      F: Monad[F]
  )(implicit A: ClassTag[A],
    run0: Ex0.Run[F],
    @transient inject: InjectTaggedK[Ex0.Repr, λ[β => F[Ex1#Repr[β]]]]): DataPipelineT[F, A, Ex1] =
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
        F.flatMap(
          source
            .evalFunc[A](Ex0)
        )(vs => inject(vs).asInstanceOf[F[Ex.Repr[B]]])
    }
}

/**
  * [[DataPipelineT]] subclass
  * with basic operations implemented:
  *
  * [[DataPipelineT.mapImpl]]      ~> [[MappingPipelineT]]
  * [[DataPipelineT.mapConcatImpl]]  ~> [[MapConcatPipelineT]]
  **/
protected[trembita] abstract class SeqSource[F[_], +A, Ex <: Environment](
    F: Monad[F]
)(implicit A: ClassTag[A])
    extends DataPipelineT[F, A, Ex] {
  def mapImpl[B: ClassTag](
      f: A => B
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new MappingPipelineT[F, A, B, Ex](f, this)(F)

  def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, Ex] =
    new MapConcatPipelineT[F, A, B, Ex](f, this)(F)

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
    new StrictSource[F, F[B]](
      iterF.map { iterator =>
        new Iterator[F[B]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[B] =
            try {
              (iterator.next(): B).pure[F]
            } catch {
              case e: Throwable => f(e)
            }
        }
      },
      F
    )(ClassTag[F[B]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)

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

object MapKPipelineT {
  def make[F[_], G[_], A, Ex <: Environment](source: DataPipelineT[F, A, Ex], ex0: Ex, arrow: F ~> G, G: Monad[G])(
      implicit A: ClassTag[A],
      run0: ex0.Run[F]
  ): DataPipelineT[G, A, Ex] =
    new SeqSource[G, A, Ex](G) {
      protected[trembita] def evalFunc[B >: A](
          Ex: Ex
      )(implicit run: Ex.Run[G]): G[Ex.Repr[B]] =
        arrow(source.evalFunc[B](ex0)).asInstanceOf[G[Ex.Repr[B]]]
    }
}

/**
  * A [[DataPipelineT]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
protected[trembita] class SortedPipelineT[+A: Ordering, F[_], Ex <: Environment](
    source: DataPipelineT[F, A, Ex],
    F: Monad[F],
    canSort: CanSort[Ex#Repr]
)(implicit A: ClassTag[A])
    extends SeqSource[F, A, Ex](F) {
  override def handleErrorImpl[B >: A: ClassTag](
      f: Throwable => B
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, Ex] =
    new SortedPipelineT[A, F, Ex](
      source
        .handleErrorImpl(f)
        .asInstanceOf[DataPipelineT[F, A, Ex]],
      F,
      canSort
    )

  protected[trembita] def evalFunc[B >: A](
      Ex: Ex
  )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
    F.map(source.evalFunc[A](Ex)) { vs =>
      canSort.sorted(vs).asInstanceOf[Ex.Repr[B]]
    }
}

object EvaluatedSource {
  def make[F[_], A: ClassTag, E <: Environment](
      reprF: F[E#Repr[A]],
      F: Monad[F]
  ): DataPipelineT[F, A, E] =
    new SeqSource[F, A, E](F) {
      override def evalFunc[B >: A](
          Ex: E
      )(implicit run: Ex.Run[F]): F[Ex.Repr[B]] =
        reprF.asInstanceOf[F[Ex.Repr[B]]]
    }
}

object MapReprPipeline {
  def make[F[_], A: ClassTag, B: ClassTag, E <: Environment](
      source: DataPipelineT[F, A, E],
      e: E
  )(f: e.Repr[A] => e.Repr[B], F: Monad[F], run: e.Run[F]): DataPipelineT[F, B, E] =
    new SeqSource[F, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[C]] =
        F.map(source.evalFunc[A](e)(run))(f).asInstanceOf[F[Ex.Repr[C]]]
    }
}

object MapReprFPipeline {
  def make[F[_], A: ClassTag, B: ClassTag, E <: Environment](
      source: DataPipelineT[F, A, E],
      e: E
  )(f: e.Repr[A] => F[e.Repr[B]], F: Monad[F], run: e.Run[F])(implicit canFlatMap: CanFlatMap[E]): DataPipelineT[F, B, E] =
    new SeqSource[F, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[C]] =
        F.flatMap(source.evalFunc[A](e)(run)) { reprA =>
          f(reprA).asInstanceOf[F[Ex.Repr[C]]]
        }
    }
}
