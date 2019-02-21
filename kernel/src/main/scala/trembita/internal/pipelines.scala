package trembita.internal

import cats._
import cats.arrow.FunctionK
import cats.implicits._
import trembita._
import trembita.operations.{CanFlatMap, CanSort, InjectTaggedK}

import scala.language.higherKinds
import scala.reflect.ClassTag

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
protected[trembita] class MappingPipelineT[F[_], Er, +A, B, E <: Environment](
    f: A => B,
    source: BiDataPipelineT[F, Er, A, E]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MappingPipelineT($f, $source)($F)($B)"

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MappingPipelineT[F, Er, A, C, E](f2.compose(f), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](a => f2(f(a)), source)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, E] =
    new HandleErrorPipelineT[F, Err, A, BB, E](f.withErrorCatched[F, Err].asInstanceOf[A => F[BB]], f2, source)(F)

  def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new HandleErrorWithPipelineT[F, Err, A, C, E](f.withErrorCatched[F, Err].asInstanceOf[A => F[C]], fallback, source)(F)

  def collectImpl[C: ClassTag](pf: PartialFunction[B, C])(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er, C, E] = new CollectPipelineT[F, Er, B, C, E](pf, this)(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.map(source.evalFunc[A](Ex))(
      vs => Ex.FlatMapRepr.map(vs)(v => v.right.map(f))
    )

  protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, B, E](source, f.withErrorCatched[F, Err], fEr)
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
protected[trembita] class MapConcatPipelineT[F[_], Er, +A, B, E <: Environment](
    f: A => Iterable[B],
    source: BiDataPipelineT[F, Er, A, E]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MapConcatPipelineT($f, $source)($F)($B)"

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](f(_).map(f2), source)(F)

  /** Each next flatMap will compose [[f]] with some other map function */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](f(_).flatMap(f2), source)(F)

  /** Filters the result of [[f]] application */
  override def filterImpl[BB >: B](
      p: B => Boolean
  )(implicit F: Monad[F], B: ClassTag[BB]): BiDataPipelineT[F, Er, BB, E] =
    new MapConcatPipelineT[F, Er, A, BB, E](f(_).filter(p), source)(F)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](f(_).collect(pf), source)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, E] =
    new HandleErrorPipelineT[F, Err, A, Iterable[BB], E](
      f.withErrorCatched[F, Err].asInstanceOf[A => F[Iterable[BB]]],
      fallback = e => List(f2(e)),
      source
    )(F).mapConcatImpl(identity)

  def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new HandleErrorWithPipelineT[F, Err, A, Iterable[C], E](
      f.withErrorCatched[F, Err].asInstanceOf[A => F[Iterable[C]]],
      fallback = e => fallback(e).map(List(_).toIterable),
      source
    )(F).mapConcatImpl(identity)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.map(source.evalFunc[A](Ex)) { repr =>
      Ex.FlatMapRepr
        .mapConcat(repr)(
          v =>
            v.right.map(f) match {
              case Left(er)  => Vector(Left(er))
              case Right(vs) => vs.map(Right(_))
          }
        )
//        .asInstanceOf[Ex.Repr[C]]
    }

  protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, Iterable[B], E](
      source,
      f.withErrorCatched[F, Err],
      fEr
    ).mapConcatImpl(identity)(implicitly, F)
}

protected[trembita] class CollectPipelineT[F[_], Er, +A, B, E <: Environment](
    pf: PartialFunction[A, B],
    source: BiDataPipelineT[F, Er, A, E]
)(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"CollectPipelineT($pf, $source)($F)($B)"

  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, A, C, E](pf.andThen(f2), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, B, C, E](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
      pf2: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, A, C, E](pf.andThen(pf2), source)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      fallback: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, E] =
    new MapConcatPipelineT[F, Err, A, B, E](pf.lift(_).toIterable, source)(F).handleErrorImpl[Err, BB](fallback)(implicitly, F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new MapConcatPipelineT[F, Err, A, B, E](pf.lift(_).toIterable, source)(F).handleErrorWithImpl[Err, C](fallback)(implicitly, F)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.map(source.evalFunc[A](Ex))(
      vs =>
        Ex.collect(vs) {
          case Left(er)                     => Left(er)
          case Right(v) if pf isDefinedAt v => Right(pf(v))
      } /*.asInstanceOf[Ex.Repr[C]]*/
    )

  protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
    new MapConcatPipelineT[F, Er2, Iterable[B], B, E](
      identity,
      source =
        new TransformErrorPipelineT[F, Err, Er2, A, Iterable[B], E](source, ((a: A) => pf.lift(a).toIterable).withErrorCatched[F, Err], fEr)
    )(F)
}

protected[trembita] class HandleErrorPipelineT[F[_], Er, +A, B, E <: Environment](
    f: A => F[B],
    fallback: Er => B,
    source: BiDataPipelineT[F, Er, A, E]
)(F: MonadError[F, Er])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"HandleErrorPipelineT($f, $fallback, $source)($F)($B)"

  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MappingPipelineT[F, Er, B, C, E](f2, this)(F)

  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, B, C, E](f2, this)(F)

  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, B, C, E](pf, this)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, E] =
    new HandleErrorPipelineT[F, Err, A, BB, E]({ a =>
      F.handleError[BB](f(a).asInstanceOf[F[BB]])(f2)
    }, f2, source)(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      fallback2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new HandleErrorWithPipelineT[F, Err, A, C, E](
      f.asInstanceOf[A => F[C]],
      e => F.handleErrorWith(F.pure(fallback(e.asInstanceOf[Er]): C))(fallback2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.flatMap(
      source
        .evalFunc[A](Ex)
    )(
      repr =>
        Ex.TraverseRepr
          .traverse(repr) {
            case Left(er) => F.pure(Left(er))
            case Right(v) => F.map(F.handleError(f(v))(fallback))(Right(_))
        }
    )

  override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, B, E](source, a => F0.handleError(f(a))(fallback.asInstanceOf[Err => B]), fEr)
}

protected[trembita] class HandleErrorWithPipelineT[F[_], Er, +A, B, E <: Environment](
    f: A => F[B],
    fallback: Er => F[B],
    source: BiDataPipelineT[F, Er, A, E]
)(F: MonadError[F, Er])(implicit B: ClassTag[B])
    extends SeqSource[F, Er, B, E](F) {

  override def toString: String = s"HandleErrorWithPipelineT($f, $fallback, $source)($F)($B)"

  override def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, E] =
    new HandleErrorWithPipelineT[F, Err, A, BB, E](
      f.asInstanceOf[A => F[BB]],
      e => fallback(e.asInstanceOf[Er]).asInstanceOf[F[BB]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      f2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new HandleErrorWithPipelineT[F, Err, A, C, E](
      f.asInstanceOf[A => F[C]],
      e => fallback(e.asInstanceOf[Er]).asInstanceOf[F[C]].handleErrorWith(f2),
      source
    )(F)

  override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, B, E](source, a => F0.handleErrorWith(f(a))(fallback.asInstanceOf[Err => F[B]]), fEr)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.flatMap(
      source
        .evalFunc[A](Ex)
    )(
      repr =>
        Ex.TraverseRepr
          .traverse(repr) {
            case Left(er) => F.pure(Left(er))
            case Right(v) => F.map(F.handleErrorWith(f(v))(fallback))(Right(_))
        }
    )
}

protected[trembita] class MapMonadicPipelineT[
    F[_],
    Er,
    +A,
    B,
    E <: Environment
](f: A => F[B], source: BiDataPipelineT[F, Er, A, E])(F: Monad[F])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MapMonadicPipelineT($f, $source)($F)($B)"

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MappingPipelineT[F, Er, B, C, E](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, B, C, E](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, B, C, E](pf, this)(F)

  def handleErrorImpl[Err >: Er, BB >: B: ClassTag](
      f2: Err => BB
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, BB, E] =
    new MapMonadicPipelineT[F, Err, A, BB, E](
      a => f(a).asInstanceOf[F[BB]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      f2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new MapMonadicPipelineT[F, Err, A, C, E](
      a => f(a).asInstanceOf[F[C]].handleErrorWith(f2),
      source
    )(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.flatMap(source.evalFunc[A](Ex)) { vs =>
      val resultF: F[Ex.Repr[Either[Er, B]]] = Ex.TraverseRepr.traverse[F, Either[Er, A], Either[Er, B]](vs) {
        case Left(er) => F.pure(Left(er))
        case Right(a) => F.map(f(a))(Right(_))
      }
      resultF.asInstanceOf[F[Ex.Repr[Either[Er, C]]]]
    }

  protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, B, E](source, f, fEr)
}

object BridgePipelineT {
  protected[trembita] def make[F[_], Er, A, E0 <: Environment, E1 <: Environment](
      source: BiDataPipelineT[F, Er, A, E0],
      Ex0: E0,
      F: Monad[F]
  )(implicit A: ClassTag[A],
    run0: Ex0.Run[F],
    @transient inject: InjectTaggedK[Ex0.Repr, λ[β => F[E1#Repr[β]]]]): BiDataPipelineT[F, Er, A, E1] =
    new SeqSource[F, Er, A, E1](F) {
      override def handleErrorImpl[Err >: Er, B >: A: ClassTag](
          f: Err => B
      )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, E1] =
        make[F, Err, B, E0, E1](source.handleErrorImpl[Err, B](f), Ex0, F)(
          implicitly[ClassTag[B]],
          run0,
          inject
        )

      override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](
          f: Err => F[B]
      )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, E1] =
        make[F, Err, B, E0, E1](source.handleErrorWithImpl[Err, B](f), Ex0, F)(
          implicitly[ClassTag[B]],
          run0,
          inject
        )

      override def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
          fEr: Err => Er2
      )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E1] =
        make[F, Er2, A, E0, E1](source.transformErrorImpl[Err, Er2](fEr), Ex0, F0)(A, run0, inject)

      protected[trembita] def evalFunc[B >: A](
          Ex: E1
      )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, B]]] =
        F.flatMap(
          source
            .evalFunc[A](Ex0)
        )(vs => inject(vs).asInstanceOf[F[Ex.Repr[Either[Er, B]]]])
    }
}

/**
  * [[BiDataPipelineT]] subclass
  * with basic operations implemented:
  *
  * [[BiDataPipelineT.mapImpl]]      ~> [[MappingPipelineT]]
  * [[BiDataPipelineT.mapConcatImpl]]  ~> [[MapConcatPipelineT]]
  **/
protected[trembita] abstract class SeqSource[F[_], Er, +A, E <: Environment](
    F: Monad[F]
)(implicit A: ClassTag[A])
    extends BiDataPipelineT[F, Er, A, E] {
  def mapImpl[B: ClassTag](
      f: A => B
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, B, E] =
    new MappingPipelineT[F, Er, A, B, E](f, this)(F)

  def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, B, E] =
    new MapConcatPipelineT[F, Er, A, B, E](f, this)(F)

  def collectImpl[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: Monad[F]): BiDataPipelineT[F, Er, B, E] =
    new CollectPipelineT[F, Er, A, B, E](pf, this)(F)
}

protected[trembita] trait SeqSourceWithImplemented[F[_], Er, +A, E <: Environment] { self: SeqSource[F, Er, A, E] =>
  override def handleErrorImpl[Err >: Er, B >: A: ClassTag](f: Err => B)(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, E] = this

  override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => F[B])(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, E] = this

  override def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      f: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E] =
    this.asInstanceOf[BiDataPipelineT[F, Er2, A, E]]
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

  override def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](fEr: Err => Er2)(
      implicit F0: MonadError[F, Err],
      F: MonadError[F, Er2]
  ): BiDataPipelineT[F, Er2, A, Sequential] =
    new StrictSource[F, Er2, F[A]](
      F0.map(iterF) { iterator =>
        new Iterator[F[A]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[A] =
            F0.handleErrorWith(((_: Unit) => iterator.next(): A).withErrorCatched[F, Err].apply(())) { err =>
              F.raiseError(fEr(err))
            }
        }
      },
      F
    )(ClassTag[F[A]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)(implicitly, F)

  /**
    * Forces evaluation of [[BiDataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](
      Ex: Sequential
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, B]]] =
    F.map(iterF)(iter => iter.map(a => Right(a: B)).toVector)

  override def toString: String = s"StrictSource(<by-name>, $F)($A, $Fctg)"
}

object MapKPipelineT {
  @`inline` def make[F[_], Er, G[_], A, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      ex0: E,
      arrow: F ~> G,
      F: Monad[F],
      G: Monad[G]
  )(
      implicit A: ClassTag[A],
      run0: ex0.Run[F]
  ): BiDataPipelineT[G, Er, A, E] =
    new SeqSource[G, Er, A, E](G) {
      protected[trembita] def evalFunc[B >: A](
          Ex: E
      )(implicit run: Ex.Run[G]): G[Ex.Repr[Either[Er, B]]] =
        arrow(source.evalFunc[B](ex0)).asInstanceOf[G[Ex.Repr[Either[Er, B]]]]

      protected[trembita] def handleErrorImpl[Err >: Er, B >: A: ClassTag](f: Err => B)(
          implicit GE: MonadError[G, Err]
      ): BiDataPipelineT[G, Err, B, E] =
        make[F, Er, G, B, E](
          source,
          ex0,
          arrow.andThen(new ~>[G, G] {
            def apply[a](fa: G[a]): G[a] = fa.handleError(f.asInstanceOf[Err => a])
          }),
          F,
          GE
        )

      protected[trembita] def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => G[B])(
          implicit GE: MonadError[G, Err]
      ): BiDataPipelineT[G, Err, B, E] =
        make[F, Er, G, B, E](
          source,
          ex0,
          arrow.andThen(new ~>[G, G] {
            def apply[a](fa: G[a]): G[a] = fa.handleErrorWith(f.asInstanceOf[Err => G[a]])
          }),
          F,
          GE
        )

      protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
          f: Err => Er2
      )(implicit GE0: MonadError[G, Err], GE1: MonadError[G, Er2]): BiDataPipelineT[G, Er2, A, E] =
        this.handleErrorWithImpl[Err, A](err => GE1.raiseError(f(err))).asInstanceOf[BiDataPipelineT[G, Er2, A, E]]

      override def toString: String = s"MapKPipelineT($source, $ex0, $arrow, $F, $G)($A, $run0)"
    }
}

/**
  * A [[BiDataPipelineT]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
protected[trembita] class SortedPipelineT[+A: Ordering, F[_], Er: ClassTag, E <: Environment](
    source: BiDataPipelineT[F, Er, A, E],
    F: Monad[F],
    canSort: CanSort[E#Repr]
)(implicit A: ClassTag[A])
    extends SeqSource[F, Er, A, E](F) {
  override def handleErrorImpl[Err >: Er, B >: A: ClassTag](
      f: Err => B
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, E] =
    new SortedPipelineT[B, F, Err, E](
      source.handleErrorImpl[Err, B](f),
      F,
      canSort
    )(Ordering[A].asInstanceOf[Ordering[B]], implicitly[ClassTag[Er]].asInstanceOf[ClassTag[Err]], A.asInstanceOf[ClassTag[B]])

  override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => F[B])(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, B, E] =
    new SortedPipelineT[B, F, Err, E](
      source.handleErrorWithImpl[Err, B](f),
      F,
      canSort
    )(Ordering[A].asInstanceOf[Ordering[B]], implicitly[ClassTag[Er]].asInstanceOf[ClassTag[Err]], A.asInstanceOf[ClassTag[B]])

  override def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      f: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E] =
    new SortedPipelineT[A, F, Er2, E](
      source.transformErrorImpl[Err, Er2](f),
      F,
      canSort
    )

  protected[trembita] def evalFunc[B >: A](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
    F.map(source.evalFunc[A](E)) { vs =>
      val (errors, values) = E.FlatMapRepr.separate(vs)
      val sorted           = canSort.sorted(values.asInstanceOf[E#Repr[A]])
      E.unite(errors, values).asInstanceOf[E.Repr[Either[Er, B]]]
    }

  override def toString: String = s"SortedPipelineT($source, $F, $canSort)($A)"
}

object EvaluatedSource {
  def make[F[_], Er, A: ClassTag, E <: Environment](
      reprF: F[E#Repr[A]],
      F: Monad[F]
  ): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      override def evalFunc[B >: A](
          Ex: E
      )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, B]]] =
        F.map(reprF)(repr => Ex.FlatMapRepr.map(repr.asInstanceOf[Ex.Repr[A]])(Right(_)))

      protected[trembita] def handleErrorImpl[Err >: Er, B >: A: ClassTag](f: Err => B)(
          implicit F: MonadError[F, Err]
      ): BiDataPipelineT[F, Err, B, E] = this
      protected[trembita] def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => F[B])(
          implicit F: MonadError[F, Err]
      ): BiDataPipelineT[F, Err, B, E] = this
      protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
          f: Err => Er2
      )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E] =
        this.asInstanceOf[BiDataPipelineT[F, Er2, A, E]]

      override def toString: String = s"EvaluatedSource($reprF, $F)(${implicitly[ClassTag[A]]})"
    }
}

object MapReprPipeline {
  def make[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      e: E
  )(f: e.Repr[A] => e.Repr[B], F: MonadError[F, Er], run: e.Run[F]): BiDataPipelineT[F, Er, B, E] =
    new SeqSource[F, Er, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] = {
        val res: F[e.Repr[B]] = F.flatMap(source.evalFunc[A](e)(run)) { repr =>
          F.map(e.TraverseRepr.traverse[F, Either[Er, A], A](repr) {
            case Left(er) => F.raiseError[A](er)
            case Right(v) => F.pure(v)
          }(implicitly, run)) { reprA =>
            f(reprA)
          }
        }
        F.map(res)(reprB => Ex.FlatMapRepr.map(reprB.asInstanceOf[Ex.Repr[B]])(Right(_)))
//          .asInstanceOf[F[Ex.Repr[Either[Er, C]]]]
      }

      protected[trembita] def handleErrorImpl[Err >: Er, C >: B: ClassTag](f: Err => C)(
          implicit F: MonadError[F, Err]
      ): BiDataPipelineT[F, Err, C, E] = this // todo: think about it

      protected[trembita] def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](f: Err => F[C])(
          implicit F: MonadError[F, Err]
      ): BiDataPipelineT[F, Err, C, E] = this // todo: think about it

      protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
          f: Err => Er2
      )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
        this.asInstanceOf[BiDataPipelineT[F, Er2, B, E]] // todo: think about it

      override def toString: String = s"MapReprPipeline($source, $e)($f, $F, $run)(${implicitly[ClassTag[A]]}, ${implicitly[ClassTag[B]]})"
    }
}

object MapReprFPipeline {
  def make[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      e: E
  )(f: e.Repr[A] => F[e.Repr[B]], F: MonadError[F, Er], run: e.Run[F])(implicit canFlatMap: CanFlatMap[E]): BiDataPipelineT[F, Er, B, E] =
    new SeqSource[F, Er, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] = {
        val res: F[e.Repr[B]] = F.flatMap(source.evalFunc[A](e)(run)) { repr =>
          F.flatMap(e.TraverseRepr.traverse[F, Either[Er, A], A](repr) {
            case Left(er) => F.raiseError[A](er)
            case Right(v) => F.pure(v)
          }(implicitly, run)) { reprA =>
            f(reprA)
          }
        }
        F.map(res)(reprB => Ex.FlatMapRepr.map(reprB.asInstanceOf[Ex.Repr[B]])(Right(_)))
      }
      protected[trembita] def handleErrorImpl[Err >: Er, C >: B: ClassTag](f: Err => C)(
          implicit F: MonadError[F, Err]
      ): BiDataPipelineT[F, Err, C, E] = this // todo: think about it

      protected[trembita] def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](f: Err => F[C])(
          implicit F: MonadError[F, Err]
      ): BiDataPipelineT[F, Err, C, E] = this // todo: think about it

      protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
          f: Err => Er2
      )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
        this.asInstanceOf[BiDataPipelineT[F, Er2, B, E]] // todo: think about it

      override def toString: String =
        s"MapReprFPipeline($source, $e)($f, $F, $run)(${implicitly[ClassTag[A]]}, ${implicitly[ClassTag[B]]}, $canFlatMap)"
    }
}

protected[trembita] case class TransformErrorPipelineT[F[_], Er: ClassTag, Er2: ClassTag, A, B: ClassTag, E <: Environment](
    source: BiDataPipelineT[F, Er, A, E],
    f: A => F[B],
    mapError: Er => Er2
)(implicit F0: MonadError[F, Er], F1: MonadError[F, Er2])
    extends BiDataPipelineT[F, Er2, B, E] {

  /**
    * Functor.map
    *
    * @tparam C - resulting data type
    * @param f2 - transformation function
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def mapImpl[C: ClassTag](f2: B => C)(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er2, C, E] = new TransformErrorPipelineT[F, Er, Er2, A, C, E](source, a => F.map(f(a))(f2), mapError)

  /**
    *
    * @tparam C - resulting data type
    * @param f2 - transformation function from [[A]] into {{{Iterable[B]}}}
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def mapConcatImpl[C: ClassTag](f2: B => Iterable[C])(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er2, C, E] =
    new MapConcatPipelineT[F, Er2, Iterable[C], C, E](
      identity,
      source = new TransformErrorPipelineT[F, Er, Er2, A, Iterable[C], E](source, a => F.map(f(a))(f2), mapError)
    )(F1)

  /**
    * Applies a [[PartialFunction]] to the [[BiDataPipelineT]]
    *
    * @tparam C - resulting data type
    * @param pf - partial function
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def collectImpl[C: ClassTag](pf: PartialFunction[B, C])(
      implicit F: Monad[F]
  ): BiDataPipelineT[F, Er2, C, E] =
    new MapConcatPipelineT[F, Er2, Iterable[C], C, E](
      identity,
      source = new TransformErrorPipelineT[F, Er, Er2, A, Iterable[C], E](source, a => F.map(f(a))(pf.lift(_).toIterable), mapError)
    )(F1)

  protected[trembita] def handleErrorImpl[Err >: Er2, C >: B: ClassTag](handle: Err => C)(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, C, E] =
    handleErrorWithImpl[Err, C](handle.andThen(F.pure))

  protected[trembita] def handleErrorWithImpl[Err >: Er2, C >: B: ClassTag](handle: Err => F[C])(
      implicit F: MonadError[F, Err]
  ): BiDataPipelineT[F, Err, C, E] =
    new HandleErrorWithPipelineT[F, Err, A, C, E](
      a => {
        val fb: F[B] = F0.handleErrorWith(f(a))(er => F1.raiseError(mapError(er)))
        fb.asInstanceOf[F[C]]
      },
      handle,
      source.asInstanceOf[BiDataPipelineT[F, Err, A, E]]
    )(F)

  protected[trembita] def transformErrorImpl[Err >: Er2: ClassTag, Er3: ClassTag](
      mapError2: Err => Er3
  )(implicit _F0: MonadError[F, Err], _F: MonadError[F, Er3]): BiDataPipelineT[F, Er3, B, E] =
    new TransformErrorPipelineT[F, Er, Er3, A, B, E](source, f, mapError andThen mapError2)

  /**
    * Forces evaluation of [[BiDataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[C >: B](Ex: E)(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er2, C]]] = {
    val frc: F[Ex.Repr[Either[Er2, C]]] =
      F0.flatMap(source.evalFunc[A](Ex)) { reprA =>
        val reprB: F[Ex.Repr[Either[Er, B]]] = Ex.TraverseRepr.traverse(reprA) {
          case Left(er) => F0.pure(Left(er))
          case Right(v) => F0.attempt(f(v))
        }
        F1.map(reprB) { reprB =>
          Ex.FlatMapRepr.map(reprB)(_.left.map(mapError).right.map(_.asInstanceOf[C]))
        }
      }

    frc
  }
}
