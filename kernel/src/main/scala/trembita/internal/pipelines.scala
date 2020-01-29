package trembita.internal

import cats.syntax.either._
import trembita._
import trembita.operations.{CanFlatMap, CanSort, InjectTaggedK}
import zio._
import scala.reflect.ClassTag

/**
  * A [[DataPipelineT]]
  * that was mapped
  *
  * @tparam A - type of source pipeline elements
  * @tparam B - type of pipeline elements after f application
  * @param f      - transformation function
  * @param source - a pipeline that f was applied on
 **/
@internalAPI
protected[trembita] class MappingPipelineT[+Er, +A, B, Ex <: Environment](
    f: A => B,
    source: DataPipelineT[Er, A, Ex]
)(implicit B: ClassTag[B])
    extends DataPipelineT[Er, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  override def mapImpl[C: ClassTag](
      f2: B => C
  ): DataPipelineT[Er, C, Ex] =
    new MappingPipelineT[Er, A, C, Ex](f2.compose(f), source)

  /** Returns [[MapConcatPipeline]] */
  override def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  ): DataPipelineT[Er, C, Ex] =
    new MapConcatPipeline[Er, A, C, Ex](a => f2(f(a)), source)

  override def catchAllImpl[BB >: B: ClassTag](
      f2: Er => BB
  ): DataPipelineT[Nothing, BB, Ex] =
    new CatchAllPipeline[Er, A, BB, Ex](f, f2, source)

  override def catchAllWithImpl[C >: B: ClassTag](
      fallback: Er => UIO[C]
  ): DataPipelineT[Nothing, C, Ex] =
    new CatchAllWithPipeline[Er, A, C, Ex](f, fallback, source)

  override def collectImpl[C: ClassTag](pf: PartialFunction[B, C]): DataPipelineT[Er, C, Ex] = new CollectPipeline[Er, B, C, Ex](pf, this)

  override protected[trembita] def evalFunc[EE >: Er, C >: B](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, C]]] =
    source
      .evalFunc[Er, A](Ex)
      .map(
        vs => Ex.FlatMapRepr.map(vs)(_.map(f))
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
@internalAPI
protected[trembita] class MapConcatPipeline[+Er, +A, B, Ex <: Environment](
    f: A => Iterable[B],
    source: DataPipelineT[Er, A, Ex]
)(implicit B: ClassTag[B])
    extends DataPipelineT[Er, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  override def mapImpl[C: ClassTag](
      f2: B => C
  ): DataPipelineT[Er, C, Ex] =
    new MapConcatPipeline[Er, A, C, Ex](f(_).map(f2), source)

  /** Each next flatMap will compose [[f]] with some other map function */
  override def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  ): DataPipelineT[Er, C, Ex] =
    new MapConcatPipeline[Er, A, C, Ex](f(_).flatMap(f2), source)

  /** Filters the result of [[f]] application */
  override def filterImpl[BB >: B: ClassTag](
      p: B => Boolean
  ): DataPipelineT[Er, BB, Ex] =
    new MapConcatPipeline[Er, A, BB, Ex](f(_).filter(p), source)

  /** Applies a [[PartialFunction]] to the result of [[f]] */
  override def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  ): DataPipelineT[Er, C, Ex] =
    new MapConcatPipeline[Er, A, C, Ex](f(_).collect(pf), source)

  override def catchAllImpl[BB >: B: ClassTag](
      f2: Er => BB
  ): DataPipelineT[Nothing, BB, Ex] =
    new CatchAllPipeline[Er, A, Iterable[BB], Ex](
      f,
      fallback = e => List(f2(e)),
      source
    ).mapConcatImpl(identity)

  override def catchAllWithImpl[C >: B: ClassTag](
      f2: Er => UIO[C]
  ): DataPipelineT[Nothing, C, Ex] = new CatchAllWithPipeline[Er, A, Iterable[C], Ex](
    f,
    fallback = e => f2(e).map(List(_)),
    source
  ).mapConcatImpl(identity)

  override protected[trembita] def evalFunc[EE >: Er, C >: B](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, C]]] =
    source.evalFunc[Er, A](Ex).map { repr =>
      Ex.FlatMapRepr.mapConcat(repr) {
        case Left(e)  => List(Left(e))
        case Right(v) => f(v).map(Right(_))
      }
    }
}

@internalAPI
class CollectPipeline[+Er, +A, B, Ex <: Environment](
    pf: PartialFunction[A, B],
    source: DataPipelineT[Er, A, Ex]
)(implicit B: ClassTag[B])
    extends DataPipelineT[Er, B, Ex] {
  override def mapImpl[C: ClassTag](
      f2: B => C
  ): DataPipelineT[Er, C, Ex] =
    new CollectPipeline[Er, A, C, Ex](pf.andThen(f2), source)

  /** Returns [[MapConcatPipeline]] */
  override def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  ): DataPipelineT[Er, C, Ex] =
    new MapConcatPipeline[Er, B, C, Ex](f2, this)

  /** Returns [[MapConcatPipeline]] with [[PartialFunction]] applied */
  override def collectImpl[C: ClassTag](
      pf2: PartialFunction[B, C]
  ): DataPipelineT[Er, C, Ex] =
    new CollectPipeline[Er, A, C, Ex](pf.andThen(pf2), source)

  override def catchAllImpl[BB >: B: ClassTag](
      f2: Er => BB
  ): DataPipelineT[Nothing, BB, Ex] = new CatchAllPipeline[Er, BB, BB, Ex](identity, f2, this)

  override def catchAllWithImpl[C >: B: ClassTag](
      fallback: Er => UIO[C]
  ): DataPipelineT[Nothing, C, Ex] =
    new CatchAllWithPipeline[Er, A, C, Ex](pf(_): C, fallback, source)

  override protected[trembita] def evalFunc[EE >: Er, C >: B](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, C]]] =
    source
      .evalFunc[Er, A](Ex)
      .map(
        vs =>
          Ex.collect(vs) {
            case Left(e)                       => Left(e)
            case Right(v) if pf.isDefinedAt(v) => Right(pf(v))
        }
      )
}

@internalAPI
protected[trembita] class CatchAllPipeline[+Er, +A, B, Ex <: Environment](
    f: A => B,
    fallback: Er => B,
    source: DataPipelineT[Er, A, Ex]
)(implicit B: ClassTag[B])
    extends DataPipelineT[Nothing, B, Ex] {

  override def mapImpl[C: ClassTag](
      f2: B => C
  ): DataPipelineT[Nothing, C, Ex] =
    new MappingPipelineT[Nothing, B, C, Ex](f2, this)

  override def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  ): DataPipelineT[Nothing, C, Ex] =
    new MapConcatPipeline[Nothing, B, C, Ex](f2, this)

  override def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  ): DataPipelineT[Nothing, C, Ex] =
    new CollectPipeline[Nothing, B, C, Ex](pf, this)

  override def catchAllImpl[BB >: B: ClassTag](
      f2: Nothing => BB
  ): DataPipelineT[Nothing, BB, Ex] = this

  override def catchAllWithImpl[C >: B: ClassTag](
      fallback2: Nothing => UIO[C]
  ): DataPipelineT[Nothing, C, Ex] = this

  override protected[trembita] def evalFunc[EE >: Nothing, C >: B](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, C]]] =
    source
      .evalFunc[Er, A](Ex)
      .flatMap { repr =>
        val res: UIO[Ex.Repr[Either[EE, C]]] = Ex.TraverseRepr
          .traverse(repr) { a =>
            val io = IO.fromEither(a)

            val x = io
              .map(f)
              .catchAll(er => IO.effectTotal(fallback(er)))

            x.widen
              .map(Right(_))
          }

        res
      }
}

@internalAPI
protected[trembita] class CatchAllWithPipeline[+Er, +A, B, Ex <: Environment](
    f: A => B,
    fallback: Er => UIO[B],
    source: DataPipelineT[Er, A, Ex]
)(implicit B: ClassTag[B])
    extends SeqSource[Nothing, B, Ex] {

  override def catchAllImpl[BB >: B: ClassTag](
      f2: Nothing => BB
  ): DataPipelineT[Nothing, BB, Ex] = this

  override def catchAllWithImpl[C >: B: ClassTag](
      f2: Nothing => UIO[C]
  ): DataPipelineT[Nothing, C, Ex] = this

  override protected[trembita] def evalFunc[EE >: Nothing, C >: B](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, C]]] =
    source.evalFunc[Er, A](Ex).flatMap { vs =>
      val res: UIO[Ex.Repr[Either[EE, C]]] = Ex.TraverseRepr
        .traverse(vs) {
          case Left(er) => fallback(er).map(Right(_))
          case Right(v) => UIO.effectTotal(f(v)).map(Right(_))
        }

      res
    }
}

@internalAPI
protected[trembita] class MapMonadicPipelineT[
    Er0,
    Er1 >: Er0,
    +A,
    B,
    Ex <: Environment
](f: A => IO[Er1, B], source: DataPipelineT[Er0, A, Ex])(implicit B: ClassTag[B])
    extends DataPipelineT[Er1, B, Ex] {

  /** Each next map will compose [[f]] with some other map function */
  override def mapImpl[C: ClassTag](
      f2: B => C
  ): DataPipelineT[Er1, C, Ex] =
    new MappingPipelineT[Er1, B, C, Ex](f2, this)

  /** Returns [[MapConcatPipeline]] */
  override def mapConcatImpl[C: ClassTag](
      f2: B => Iterable[C]
  ): DataPipelineT[Er1, C, Ex] =
    new MapConcatPipeline[Er1, B, C, Ex](f2, this)

  /** Returns [[MapConcatPipeline]] with [[PartialFunction]] applied */
  override def collectImpl[C: ClassTag](
      pf: PartialFunction[B, C]
  ): DataPipelineT[Er1, C, Ex] =
    new CollectPipeline[Er1, B, C, Ex](pf, this)

  override def catchAllImpl[BB >: B: ClassTag](
      f2: Er1 => BB
  ): DataPipelineT[Nothing, BB, Ex] =
    new MapMonadicPipelineT[Er0, Nothing, A, BB, Ex](
      a => f(a).catchAll(er1 => ZIO.effectTotal(f2(er1))),
      source
    )

  override def catchAllWithImpl[C >: B: ClassTag](
      f2: Er1 => UIO[C]
  ): DataPipelineT[Nothing, C, Ex] =
    new MapMonadicPipelineT[Er0, Nothing, A, C, Ex](
      a => f(a).catchAll(f2),
      source
    )

  override protected[trembita] def evalFunc[EE >: Er1, C >: B](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, C]]] =
    source.evalFunc[Er0, A](Ex).flatMap { vs =>
      val resultF = Ex.TraverseRepr.traverse(vs) {
        case Left(e)  => UIO.succeed(e.asLeft)
        case Right(v) => f(v).widen[EE, C].either
      }
      resultF
    }
}

@internalAPI
object BridgePipelineT {
  protected[trembita] def make[Er, A, Ex0 <: Environment, Ex1 <: Environment](
      source: DataPipelineT[Er, A, Ex0],
      Ex0: Ex0
  )(implicit A: ClassTag[A], @transient inject: InjectTaggedK[Ex0.Repr, λ[β => UIO[Ex1#Repr[β]]]]): DataPipelineT[Er, A, Ex1] =
    new SeqSource[Er, A, Ex1] {
      override def catchAllImpl[B >: A: ClassTag](
          f: Er => B
      ): DataPipelineT[Nothing, B, Ex1] =
        make[Nothing, B, Ex0, Ex1](source.catchAllImpl(f), Ex0)(
          implicitly[ClassTag[B]],
          inject
        )

      override def catchAllWithImpl[B >: A: ClassTag](
          f: Er => UIO[B]
      ): DataPipelineT[Nothing, B, Ex1] =
        make[Nothing, B, Ex0, Ex1](source.catchAllWithImpl(f), Ex0)(
          implicitly[ClassTag[B]],
          inject
        )

      override protected[trembita] def evalFunc[EE >: Er, B >: A](
          Ex: Ex1
      ): UIO[Ex.Repr[Either[EE, B]]] =
        source
          .evalFunc[Er, A](Ex0)
          .flatMap { vs =>
            val res = inject(vs)
            res.widenEither[Ex.Repr, EE, B]
          }
    }
}

/**
  * [[DataPipelineT]] subclass
  * with basic operations implemented:
  *
  * [[DataPipelineT.mapImpl]]      ~> [[MappingPipelineT]]
  * [[DataPipelineT.mapConcatImpl]]  ~> [[MapConcatPipeline]]
 **/
@internalAPI
protected[trembita] abstract class SeqSource[+Er, +A, Ex <: Environment](implicit A: ClassTag[A]) extends DataPipelineT[Er, A, Ex] {

  override def mapImpl[B: ClassTag](
      f: A => B
  ): DataPipelineT[Er, B, Ex] =
    new MappingPipelineT[Er, A, B, Ex](f, this)

  override def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  ): DataPipelineT[Er, B, Ex] =
    new MapConcatPipeline[Er, A, B, Ex](f, this)

  override def collectImpl[B: ClassTag](
      pf: PartialFunction[A, B]
  ): DataPipelineT[Er, B, Ex] =
    new CollectPipeline[Er, A, B, Ex](pf, this)

  override protected[trembita] def catchAllImpl[B >: A: ClassTag](f: Er => B): DataPipelineT[Nothing, B, Ex] =
    new CatchAllPipeline[Er, B, B, Ex](identity, f, this)

  override protected[trembita] def catchAllWithImpl[B >: A: ClassTag](f: Er => UIO[B]): DataPipelineT[Nothing, B, Ex] =
    new CatchAllWithPipeline[Er, B, B, Ex](identity, f, this)
}

/**
  * Concrete implementation of [[DataPipelineT]]
  * wrapping by-name [[Iterable]]
  *
  * @tparam A - type of pipeline elements
  * @param iterF - not evaluated yet collection of [[A]]
 **/
@internalAPI
protected[trembita] class StrictSource[+Er, +A](
    iterF: UIO[Iterator[Either[Er, A]]],
)(implicit A: ClassTag[A])
    extends SeqSource[Er, A, Sequential] {

  override def catchAllImpl[B >: A: ClassTag](
      f: Er => B
  ): DataPipelineT[Nothing, B, Sequential] =
    new StrictSource[Nothing, B](iterF.map { iter =>
      iter.map {
        case Left(er) => Right(f(er))
        case Right(v) => Right(v)
      }
    })

  override def catchAllWithImpl[B >: A: ClassTag](
      f: Er => UIO[B]
  ): DataPipelineT[Nothing, B, Sequential] =
    new StrictSource[Nothing, UIO[B]](
      iterF.map { iter =>
        iter.map {
          case Left(er) => Right(f(er))
          case Right(v) => Right(UIO.succeed[B](v))
        }
      }
    ).mapMImpl(fb => fb)

  /**
    * Forces evaluation of [[DataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
   **/
  override protected[trembita] def evalFunc[EE >: Er, B >: A](
      Ex: Sequential
  ): UIO[Ex.Repr[Either[EE, B]]] =
    iterF.map(_.toVector)
}

/**
  * A [[DataPipelineT]] been sorted
  *
  * @tparam A - type of pipeline elements (requires [[Ordering]] and [[ClassTag]])
  * @param source - source pipeline to be sorted
  **/
@internalAPI
protected[trembita] class SortedPipelineT[+A: Ordering, Er, Ex <: Environment](
    source: DataPipelineT[Er, A, Ex],
    canSort: CanSort[Ex#Repr]
)(implicit A: ClassTag[A])
    extends SeqSource[Er, A, Ex] {

  override def catchAllImpl[B >: A: ClassTag](
      f: Er => B
  ): DataPipelineT[Nothing, B, Ex] =
    new SortedPipelineT[A, Nothing, Ex](
      source
        .catchAllImpl(f),
      canSort
    )(eitherOrdering[Nothing, A], A)

  override protected[trembita] def evalFunc[EE >: Er, B >: A](
      Ex: Ex
  ): UIO[Ex.Repr[Either[EE, B]]] =
    source
      .evalFunc[Er, A](Ex)
      .map { vs =>
        canSort.sorted[Either[Er, A]](vs.asInstanceOf[Ex#Repr[Either[Er, A]]])(eitherOrdering[Er, A], implicitly[ClassTag[Either[Er, A]]])
      }
      .widenEither
}

@internalAPI
object EvaluatedSource {
  def make[Er, A: ClassTag, E <: Environment](
      reprF: UIO[E#Repr[A]]
  ): DataPipelineT[Er, A, E] =
    new SeqSource[Er, A, E] {
      override def evalFunc[EE >: Er, B >: A](
          Ex: E
      ): UIO[Ex.Repr[Either[EE, B]]] =
        reprF.map(vs => Ex.FlatMapRepr.map[A, Either[EE, B]](vs.asInstanceOf[Ex.Repr[A]])(_.asRight))
    }
}

@internalAPI
object MapReprPipeline {
  def make[Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: DataPipelineT[Er, A, E],
      e: E
  )(f: e.Repr[Either[Er, A]] => e.Repr[Either[Er, B]]): DataPipelineT[Er, B, E] =
    new SeqSource[Er, B, E] {
      override def evalFunc[EE >: Er, C >: B](
          Ex: E
      ): UIO[Ex.Repr[Either[EE, C]]] =
        source
          .evalFunc[Er, A](e)
          .map { vs =>
            f(vs)
          }
          .widenEither
    }
}

@internalAPI
object MapReprFPipeline {
  def make[Er, Er2 >: Er, A: ClassTag, B: ClassTag, E <: Environment](
      source: DataPipelineT[Er, A, E],
      e: E
  )(f: e.Repr[Either[Er, A]] => UIO[e.Repr[Either[Er2, B]]])(implicit canFlatMap: CanFlatMap[E]): DataPipelineT[Er2, B, E] =
    new SeqSource[Er2, B, E] {
      override def evalFunc[EE >: Er2, C >: B](
          Ex: E
      ): UIO[Ex.Repr[Either[EE, C]]] =
        source.evalFunc[Er, A](e).flatMap { reprA =>
          f(reprA).widenEither
        }
    }
}
