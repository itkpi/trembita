package trembita.internal

import cats._
import cats.arrow.FunctionK
import cats.implicits._
import trembita._
import trembita.operations.{CanFlatMap, CanSort, InjectTaggedK}

import scala.annotation.unchecked.uncheckedVariance
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
)(F: MonadError[F, Er @uncheckedVariance])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MappingPipelineT($f, $source)($F)($B)"

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MappingPipelineT[F, Er, A, C, E](f2.compose(f), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](a => f2(f(a)), source)(F)

  def collectImpl[C: ClassTag](pf: PartialFunction[B, C])(
      implicit F: MonadError[F, Er @uncheckedVariance]
  ): BiDataPipelineT[F, Er, C, E] = new CollectPipelineT[F, Er, B, C, E](pf, this)(F)

  protected[trembita] def evalFunc[BB >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, BB]]] =
    F.flatMap(source.evalFunc[A](Ex))(
      vs =>
        Ex.TraverseRepr.traverse(vs)(
          _.fold(
            er => F.pure(Left(er)),
            v =>
              F.attempt(
                F.map(F.pure(v))(f)
            )
          )
      )
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
protected[trembita] class MapConcatPipelineT[F[_], Er, +A, B, E <: Environment](
    f: A => Iterable[B],
    source: BiDataPipelineT[F, Er, A, E]
)(F: MonadError[F, Er @uncheckedVariance])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MapConcatPipelineT($f, $source)($F)($B)"

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](f(_).map(f2), source)(F)

  /** Each next flatMap will compose [[f]] with some other map function */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](f(_).flatMap(f2), source)(F)

  /** Filters the result of [[f]] application */
  override def filterImpl[BB >: B](
      p: B => Boolean
  )(implicit F: MonadError[F, Er @uncheckedVariance], B: ClassTag[BB]): BiDataPipelineT[F, Er, BB, E] =
    new MapConcatPipelineT[F, Er, A, BB, E](f(_).filter(p), source)(F)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, A, C, E](f(_).collect(pf), source)(F)

  protected[trembita] def evalFunc[C >: B](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, C]]] = {
    val x = F.flatMap(source.evalFunc[A](E)) { repr =>
      E.TraverseRepr
        .traverse[F, Either[Er, A], Either[Er, Iterable[Either[Er, C]]]](repr) {
          case Left(er) => F.pure(Right(Vector(Left(er))))
          case Right(v) => F.attempt(F.map(F.pure(v))(f(_).map(_.asRight[Er])))
        }
    }

    F.map(x)(vs => E.FlatMapRepr.mapConcat(vs)(_.fold(er => Vector(Left(er)), identity)))
  }
}

protected[trembita] class CollectPipelineT[F[_], Er, +A, B, E <: Environment](
    pf: PartialFunction[A, B],
    source: BiDataPipelineT[F, Er, A, E]
)(F: MonadError[F, Er @uncheckedVariance])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"CollectPipelineT($pf, $source)($F)($B)"

  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, A, C, E](pf.andThen(f2), source)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, B, C, E](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
      pf2: PartialFunction[B, C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, A, C, E](pf.andThen(pf2), source)(F)

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
}

protected[trembita] class HandleErrorPipelineT[F[_], Er, A, E <: Environment](
    fallback: Er => A,
    source: BiDataPipelineT[F, Er, A, E]
)(F: MonadError[F, Er])(implicit B: ClassTag[A])
    extends BiDataPipelineT[F, Er, A, E] {

  override def toString: String = s"HandleErrorPipelineT($fallback, $source)($F)($B)"

  def mapImpl[B: ClassTag](
      f2: A => B
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, B, E] =
    new MappingPipelineT[F, Er, A, B, E](f2, this)(F)

  def mapConcatImpl[B: ClassTag](
      f2: A => Iterable[B]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, B, E] =
    new MapConcatPipelineT[F, Er, A, B, E](f2, this)(F)

  def collectImpl[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, B, E] =
    new CollectPipelineT[F, Er, A, B, E](pf, this)(F)

  override def handleErrorImpl[Err >: Er, AA >: A: ClassTag](
      f2: Err => AA
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, AA, E] =
    new HandleErrorWithPipelineT[F, Err, AA, E](
      e => F.raiseError[AA](e.asInstanceOf[Er]).handleError(e => fallback(e.asInstanceOf[Er])).handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[Err >: Er, AA >: A: ClassTag](
      fallback2: Err => F[AA]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, AA, E] =
    new HandleErrorWithPipelineT[F, Err, AA, E](
      (e: Err) => F.handleErrorWith(F.pure(fallback(e.asInstanceOf[Er]): AA))(fallback2),
      source
    )(F)

  protected[trembita] def evalFunc[AA >: A](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, AA]]] =
    F.handleError[E.Repr[Either[Er, AA]]](
      F.map(
        source
          .evalFunc[A](E)
      )(
        repr =>
          E.FlatMapRepr
            .map(repr) {
              case Left(er) => Right(fallback(er))
              case Right(v) => Right(v)
          }
      )
    )(err => E.FlatMapRepr.pure(Right(fallback(err))))

  override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, E](this, fEr)
}

protected[trembita] class HandleErrorWithPipelineT[F[_], Er, +A, E <: Environment](
    fallback: Er => F[A],
    source: BiDataPipelineT[F, Er, A, E]
)(F: MonadError[F, Er])(implicit A: ClassTag[A])
    extends SeqSource[F, Er, A, E](F) { self =>

  override def toString: String = s"HandleErrorWithPipelineT($fallback, $source)($F)($A)"

  override def handleErrorImpl[Err >: Er, AA >: A: ClassTag](
      f2: Err => AA
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, AA, E] =
    new HandleErrorWithPipelineT[F, Err, AA, E](
      e => fallback(e.asInstanceOf[Er]).asInstanceOf[F[AA]].handleError(f2),
      source
    )(F)

  override def handleErrorWithImpl[Err >: Er, AA >: A: ClassTag](
      f2: Err => F[AA]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, AA, E] =
    new HandleErrorWithPipelineT[F, Err, AA, E](
      e => fallback(e.asInstanceOf[Er]).asInstanceOf[F[AA]].handleErrorWith(f2),
      source
    )(F)

  override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
      fEr: Err => Er2
  )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, A, E] =
    new TransformErrorPipelineT[F, Err, Er2, A, E](this, fEr)

  protected[trembita] def evalFunc[AA >: A](
      E: E
  )(implicit run: E.Run[F]): F[E.Repr[Either[Er, AA]]] =
    F.handleErrorWith[E.Repr[Either[Er, AA]]](
      F.flatMap(
        source
          .evalFunc[A](E)
      )(
        repr =>
          E.TraverseRepr
            .traverse(repr) {
              case Left(er) => F.attempt(fallback(er).asInstanceOf[F[AA]])
              case Right(v) => F.pure(Right(v.asInstanceOf[AA]))
          }
      )
    )(err => F.map(F.attempt(fallback(err)))(res => E.FlatMapRepr.pure(res)))
}

protected[trembita] class MapMonadicPipelineT[
    F[_],
    Er,
    +A,
    B,
    E <: Environment
](f: A => F[B], source: BiDataPipelineT[F, Er, A, E])(F: MonadError[F, Er @uncheckedVariance])(implicit B: ClassTag[B])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MapMonadicPipelineT($f, $source)($F)($B)"

  /** Each next map will compose [[f]] with some other map function */
  def mapImpl[C: ClassTag](
      f2: B => C
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MappingPipelineT[F, Er, B, C, E](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] */
  def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new MapConcatPipelineT[F, Er, B, C, E](f2, this)(F)

  /** Returns [[MapConcatPipelineT]] with [[PartialFunction]] applied */
  def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, C, E] =
    new CollectPipelineT[F, Er, B, C, E](pf, this)(F)

  override def handleErrorWithImpl[Err >: Er, C >: B: ClassTag](
      f2: Err => F[C]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, C, E] =
    new HandleErrorWithPipelineT[F, Err, C, E](f2, this)(F)

  protected[trembita] def evalFunc[C >: B](
      Ex: E
  )(implicit run: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] =
    F.flatMap(source.evalFunc[A](Ex)) { vs =>
      val resultF: F[Ex.Repr[Either[Er, B]]] = Ex.TraverseRepr.traverse[F, Either[Er, A], Either[Er, B]](vs) {
        case Left(er) => F.pure(Left(er))
        case Right(a) => F.attempt(f(a))
      }
      resultF.asInstanceOf[F[Ex.Repr[Either[Er, C]]]]
    }
}

object BridgePipelineT {
  protected[trembita] def make[F[_], Er, A, E0 <: Environment, E1 <: Environment](
      source: BiDataPipelineT[F, Er, A, E0],
      Ex0: E0,
      F: MonadError[F, Er @uncheckedVariance]
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
        make[F, Er2, A, E0, E1](source.transformErrorImpl[Err, Er2](fEr), Ex0, F)(A, run0, inject)

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
    F: MonadError[F, Er @uncheckedVariance]
)(implicit A: ClassTag[A])
    extends BiDataPipelineT[F, Er, A, E] {
  def mapImpl[B: ClassTag](
      f: A => B
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, B, E] =
    new MappingPipelineT[F, Er, A, B, E](f, this)(F)

  def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, B, E] =
    new MapConcatPipelineT[F, Er, A, B, E](f, this)(F)

  def collectImpl[B: ClassTag](
      pf: PartialFunction[A, B]
  )(implicit F: MonadError[F, Er @uncheckedVariance]): BiDataPipelineT[F, Er, B, E] =
    new CollectPipelineT[F, Er, A, B, E](pf, this)(F)
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
    F: MonadError[F, Er @uncheckedVariance]
)(implicit A: ClassTag[A], Fctg: ClassTag[F[_]])
    extends SeqSource[F, Er, A, Sequential](F) {
  override def handleErrorImpl[Err >: Er, B >: A: ClassTag](
      f: Err => B
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Sequential] =
    new StrictSource[F, Err, F[B]](
      F.handleError(iterF.map { iterator =>
        new Iterator[F[B]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[B] =
            ((_: Unit) => iterator.next(): B).withErrorCatched[F, Err].apply(()).handleError(f)
        }
      })(err => Iterator.single(F.pure(f(err)))),
      F
    )(ClassTag[F[B]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)

  override def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](
      f: Err => F[B]
  )(implicit F: MonadError[F, Err]): BiDataPipelineT[F, Err, B, Sequential] =
    new StrictSource[F, Err, F[B]](
      F.handleError(iterF map { iterator =>
        new Iterator[F[B]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[B] =
            ((_: Unit) => iterator.next(): B).withErrorCatched[F, Err].apply(()).handleErrorWith(f)
        }
      })(err => Iterator.single(f(err))),
      F
    )(ClassTag[F[B]](Fctg.runtimeClass), Fctg).mapMImpl(fb => fb)

  override def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](fEr: Err => Er2)(
      implicit F0: MonadError[F, Err],
      F: MonadError[F, Er2]
  ): BiDataPipelineT[F, Er2, A, Sequential] =
    new StrictSource[F, Er2, F[A]](
      F0.handleError(F0.map(iterF) { iterator =>
        new Iterator[F[A]] {
          def hasNext: Boolean = iterator.hasNext

          def next(): F[A] =
            F0.handleErrorWith(((_: Unit) => iterator.next(): A).withErrorCatched[F, Err].apply(())) { err =>
              F.raiseError(fEr(err))
            }
        }
      })(err => Iterator.single(F.raiseError[A](fEr(err)))),
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
    F.map(iterF)(iter => iter.map(_.asRight[Er]).toVector)

  override def toString: String = s"StrictSource(<by-name>, $F)($A, $Fctg)"
}

object MapKPipelineT {
  @`inline` def make[F[_], Er, G[_], A, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      ex0: E,
      arrow: F ~> G,
      F: MonadError[F, Er @uncheckedVariance],
      G: MonadError[G, Er @uncheckedVariance]
  )(
      implicit A: ClassTag[A],
      run0: ex0.Run[F]
  ): BiDataPipelineT[G, Er, A, E] =
    new SeqSource[G, Er, A, E](G) {
      protected[trembita] def evalFunc[B >: A](
          E: E
      )(implicit run: E.Run[G]): G[E.Repr[Either[Er, B]]] =
        arrow(source.evalFunc[B](ex0)).asInstanceOf[G[E.Repr[Either[Er, B]]]]

      override protected[trembita] def handleErrorImpl[Err >: Er, B >: A: ClassTag](f: Err => B)(
          implicit GE: MonadError[G, Err]
      ): BiDataPipelineT[G, Err, B, E] =
        make[F, Err, G, B, E](
          source,
          ex0,
          arrow.andThen(new ~>[G, G] {
            def apply[a](fa: G[a]): G[a] = fa.handleError(f.asInstanceOf[Err => a])
          }),
          F.asInstanceOf[MonadError[F, Err]],
          GE
        )

      override protected[trembita] def handleErrorWithImpl[Err >: Er, B >: A: ClassTag](f: Err => G[B])(
          implicit GE: MonadError[G, Err]
      ): BiDataPipelineT[G, Err, B, E] =
        make[F, Err, G, B, E](
          source,
          ex0,
          arrow.andThen(new ~>[G, G] {
            def apply[a](fa: G[a]): G[a] = fa.handleErrorWith(f.asInstanceOf[Err => G[a]])
          }),
          F.asInstanceOf[MonadError[F, Err]],
          GE
        )

      override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
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
    F: MonadError[F, Er @uncheckedVariance],
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
      E.unite(errors, sorted.asInstanceOf[E.Repr[A]]).asInstanceOf[E.Repr[Either[Er, B]]]
    }

  override def toString: String = s"SortedPipelineT($source, $F, $canSort)($A)"
}

object EvaluatedSource {
  def makePure[F[_], Er, A: ClassTag, E <: Environment](
      reprF: F[E#Repr[A]],
      F: MonadError[F, Er @uncheckedVariance]
  ): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      override def evalFunc[B >: A](
          E: E
      )(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
        F.map(reprF)(repr => E.FlatMapRepr.map(repr.asInstanceOf[E.Repr[A]])(_.asRight[Er]))

      override def toString: String = s"EvaluatedSource($reprF, $F)(${implicitly[ClassTag[A]]})"
    }

  def make[F[_], Er, A: ClassTag, E <: Environment](
      reprF: F[E#Repr[Either[Er, A]]],
      F: MonadError[F, Er @uncheckedVariance]
  ): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      override def evalFunc[B >: A](
          E: E
      )(implicit run: E.Run[F]): F[E.Repr[Either[Er, B]]] =
        reprF.asInstanceOf[F[E.Repr[Either[Er, B]]]]

      override def toString: String = s"EvaluatedSource($reprF, $F)(${implicitly[ClassTag[A]]})"
    }
}

object MapReprPipeline {
  def makePure[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
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

      override def toString: String = s"MapReprPipeline($source, $e)($f, $F, $run)(${implicitly[ClassTag[A]]}, ${implicitly[ClassTag[B]]})"
    }
}

object MapReprFPipeline {
  @`inline` def makePure[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      e: E
  )(f: e.Repr[A] => F[e.Repr[B]], F: MonadError[F, Er], run: e.Run[F]): BiDataPipelineT[F, Er, B, E] =
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

      override def toString: String =
        s"MapReprFPipeline.makePure($source, $e)($f, $F, $run)(${implicitly[ClassTag[A]]}, ${implicitly[ClassTag[B]]})"
    }

  @`inline` def make[F[_], Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: BiDataPipelineT[F, Er, A, E],
      e: E
  )(f: e.Repr[Either[Er, A]] => F[e.Repr[Either[Er, B]]], F: MonadError[F, Er], run: e.Run[F]): BiDataPipelineT[F, Er, B, E] =
    new SeqSource[F, Er, B, E](F) {
      override def evalFunc[C >: B](
          Ex: E
      )(implicit run0: Ex.Run[F]): F[Ex.Repr[Either[Er, C]]] = {
        val res: F[e.Repr[Either[Er, B]]] = F.flatMap(source.evalFunc[A](e)(run)) { repr =>
          f(repr)
        }
        res.asInstanceOf[F[Ex.Repr[Either[Er, C]]]]
      }

      override protected[trembita] def transformErrorImpl[Err >: Er: ClassTag, Er2: ClassTag](
          fErr: Err => Er2
      )(implicit F0: MonadError[F, Err], F: MonadError[F, Er2]): BiDataPipelineT[F, Er2, B, E] =
        make[F, Er, A, B, E](source, e)(
          repr => F0.map(f(repr))(reprB => e.FlatMapRepr.map(reprB)(_.left.map(fErr)).asInstanceOf[e.Repr[Either[Er, B]]]),
          F0.asInstanceOf[MonadError[F, Er]],
          run
        ).asInstanceOf[BiDataPipelineT[F, Er2, B, E]]

      override def toString: String =
        s"MapReprFPipeline.make($source, $e)($f, $F, $run)(${implicitly[ClassTag[A]]}, ${implicitly[ClassTag[B]]})"
    }
}

protected[trembita] case class TransformErrorPipelineT[F[_], Er: ClassTag, Er2: ClassTag, A, E <: Environment](
    source: BiDataPipelineT[F, Er, A, E],
    mapError: Er => Er2
)(implicit F0: MonadError[F, Er], F1: MonadError[F, Er2])
    extends BiDataPipelineT[F, Er2, A, E] {

  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f2 - transformation function
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def mapImpl[B: ClassTag](f2: A => B)(
      implicit F: MonadError[F, Er2 @uncheckedVariance]
  ): BiDataPipelineT[F, Er2, B, E] =
    new MappingPipelineT[F, Er2, A, B, E](f2, this)(F)

  /**
    *
    * @tparam B - resulting data type
    * @param f2 - transformation function from [[A]] into {{{Iterable[B]}}}
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def mapConcatImpl[B: ClassTag](f2: A => Iterable[B])(
      implicit F: MonadError[F, Er2 @uncheckedVariance]
  ): BiDataPipelineT[F, Er2, B, E] =
    new MapConcatPipelineT[F, Er2, A, B, E](f2, this)(F1)

  /**
    * Applies a [[PartialFunction]] to the [[BiDataPipelineT]]
    *
    * @tparam C - resulting data type
    * @param pf - partial function
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def collectImpl[C: ClassTag](pf: PartialFunction[A, C])(
      implicit F: MonadError[F, Er2 @uncheckedVariance]
  ): BiDataPipelineT[F, Er2, C, E] =
    new CollectPipelineT[F, Er2, A, C, E](pf, this)(F)

  override protected[trembita] def transformErrorImpl[Err >: Er2: ClassTag, Er3: ClassTag](
      mapError2: Err => Er3
  )(implicit _F0: MonadError[F, Err], _F: MonadError[F, Er3]): BiDataPipelineT[F, Er3, A, E] =
    new TransformErrorPipelineT[F, Er, Er3, A, E](source, mapError andThen mapError2)

  /**
    * Forces evaluation of [[BiDataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[AA >: A](E: E)(implicit run: E.Run[F]): F[E.Repr[Either[Er2, AA]]] = {
    val frc: F[E.Repr[Either[Er2, AA]]] =
      F0.map(source.evalFunc[A](E)) { reprA =>
        val reprB: E.Repr[Either[Er2, A]] = E.FlatMapRepr.map(reprA)(_.left.map(mapError))
        reprB.asInstanceOf[E.Repr[Either[Er2, AA]]]
      }

    F0.handleError(frc)(err => E.FlatMapRepr.pure(Left(mapError(err))))
  }
}

class MapConcatMPipeline[F[_], Er, A, B: ClassTag, E <: Environment](
    source: BiDataPipelineT[F, Er, A, E],
    f: A => F[Iterable[B]]
)(implicit F: MonadError[F, Er @uncheckedVariance])
    extends BiDataPipelineT[F, Er, B, E] {

  override def toString: String = s"MapConcatMPipeline($source, $f)($F)"

  /**
    * Functor.map
    *
    * @tparam C - resulting data type
    * @param f2 - transformation function
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def mapImpl[C: ClassTag](f2: B => C)(
      implicit F: MonadError[F, Er @uncheckedVariance]
  ): BiDataPipelineT[F, Er, C, E] = new MapConcatMPipeline[F, Er, A, C, E](
    source,
    a => F.map(f(a))(_ map f2)
  )

  /**
    *
    * @tparam C - resulting data type
    * @param f2 - transformation function from [[A]] into {{{Iterable[C]}}}
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def mapConcatImpl[C: ClassTag](f2: B => Iterable[C])(
      implicit F: MonadError[F, Er @uncheckedVariance]
  ): BiDataPipelineT[F, Er, C, E] = new MapConcatMPipeline[F, Er, A, C, E](
    source,
    a => F.map(f(a))(_ flatMap f2)
  )

  /**
    * Applies a [[PartialFunction]] to the [[BiDataPipelineT]]
    *
    * @tparam C - resulting data type
    * @param pf - partial function
    * @return - transformed [[BiDataPipelineT]]
    **/
  protected[trembita] def collectImpl[C: ClassTag](pf: PartialFunction[B, C])(
      implicit F: MonadError[F, Er @uncheckedVariance]
  ): BiDataPipelineT[F, Er, C, E] = new MapConcatMPipeline[F, Er, A, C, E](
    source,
    a => F.map(f(a))(_ collect pf)
  )

  override def mapMImpl[BB >: B, C: ClassTag](f2: B => F[C])(
      implicit F: MonadError[F, Er]
  ): BiDataPipelineT[F, Er, C, E] = new MapConcatMPipeline[F, Er, A, C, E](
    source,
    a => F.flatMap(f(a))(bs => bs.toVector.traverse[F, C](f2)(F).asInstanceOf[F[Iterable[C]]])
  )

  /**
    * Forces evaluation of [[BiDataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[BB >: B](E: E)(implicit run: E.Run[F]): F[E.Repr[Either[Er, BB]]] = F.flatMap(source.evalFunc[A](E)) {
    repr =>
      F.map(E.TraverseRepr.traverse[F, Either[Er, A], Either[Er, Iterable[B]]](repr) {
        case Left(er) => F.pure(Left(er))
        case Right(v) => F.attempt(f(v))
      })(
        mapped =>
          E.FlatMapRepr.mapConcat(mapped) {
            case Left(er)  => Vector(Left(er))
            case Right(vs) => vs.map(_.asRight[Er])
        }
      )
  }
}
