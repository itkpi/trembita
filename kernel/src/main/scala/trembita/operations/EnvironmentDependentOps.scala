package trembita.operations

import cats._
import cats.implicits._
import trembita.internal._
import trembita.{BiDataPipelineT, Environment}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait EnvironmentDependentOps[F[_], Er, A, E <: Environment] extends Any {
  def `this`: BiDataPipelineT[F, Er, A, E]

  /**
    * Evaluates environment-specific data representation
    * */
  @internalAPI("Used in output DSL. Try to use output DSL instead because in most cases it allows to do what you want")
  def evalRepr(implicit E: E, run: E#Run[F]): F[E.Repr[A]] =
    `this`.evalFunc[A](E)(widen(run)(E))

  /**
    * Takes only first N elements of the pipeline
    *
    * @param n - number of elements to take
    * @return - first N elements
    **/
  def take(n: Int)(implicit F: Monad[F], canTake: CanTake[E#Repr], A: ClassTag[A], E: E, Run: E#Run[F]): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      protected[trembita] def evalFunc[B >: A](
          ex0: E
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(`this`.evalFunc[B](E)(widen(Run)(E)))(canTake.take(_, n))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  /**
    * Take all elements of the pipeline
    * dropping first N elements
    *
    * @param n - number of elements to drop
    * @return - all elements with first N dropped
      **/
  def drop(n: Int)(implicit F: Monad[F], canDrop: CanDrop[E#Repr], A: ClassTag[A], E: E, Run: E#Run[F]): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      protected[trembita] def evalFunc[B >: A](
          ex0: E
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(`this`.evalFunc[B](E)(widen(Run)(E)))(canDrop.drop(_, n))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  def slice(from: Int,
            to: Int)(implicit F: Monad[F], canSlice: CanSlice[E#Repr], A: ClassTag[A], E: E, Run: E#Run[F]): BiDataPipelineT[F, Er, A, E] =
    new SeqSource[F, Er, A, E](F) {
      protected[trembita] def evalFunc[B >: A](
          ex0: E
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(
            `this`
              .evalFunc[B](E)(widen(Run)(E))
          )(canSlice.slice(_, from, to))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  /**
    * Allows to run shift pipeline transformations into other environment
    * (for instance from local to spark)
    * */
  def to[Ex2 <: Environment](
      implicit E: E,
      run1: E#Run[F],
      A: ClassTag[A],
      F: Monad[F],
      injectK: InjectTaggedK[E#Repr, Ex2#Repr]
  ): BiDataPipelineT[F, Er, A, Ex2] =
    BridgePipelineT.make[F, Er, A, E, Ex2](`this`, E, F)(
      A,
      widen(run1)(E),
      InjectTaggedK
        .fromId[F, E#Repr, Ex2#Repr](injectK)
        .asInstanceOf[InjectTaggedK[E.Repr, λ[β => F[Ex2#Repr[β]]]]]
    )

  /**
    * Allows to run shift pipeline transformations into other environment
    * (for instance from akka to spark).
    * `F` suffix means that environment switching is side-effectful or asynchronous operation
    * */
  def toF[Ex2 <: Environment](
      implicit E: E,
      run1: E#Run[F],
      A: ClassTag[A],
      F: Monad[F],
      injectK: InjectTaggedK[E#Repr, λ[β => F[Ex2#Repr[β]]]]
  ): BiDataPipelineT[F, Er, A, Ex2] =
    BridgePipelineT.make[F, Er, A, E, Ex2](`this`, E, F)(
      A,
      widen(run1)(E),
      injectK.asInstanceOf[InjectTaggedK[E.Repr, λ[β => F[Ex2#Repr[β]]]]]
    )

  /**
    * Allows to change pipeline evaluation context
    * (for instance from IO to scala.concurrent.Future)
    * */
  def mapK[G[_]](arrow: F ~> G)(implicit G: Monad[G], E: E, run0: E#Run[F], A: ClassTag[A]): BiDataPipelineT[G, Er, A, E] =
    MapKPipelineT.make[F, Er, G, A, E](`this`, E, arrow, G)(A, widen(run0)(E))

  /**
    * Orders elements of the [[BiDataPipelineT]]
    * having an [[Ordering]] defined for type [[A]]
    *
    * @return - the same pipeline sorted
    **/
  def sorted(implicit F: Monad[F], A: ClassTag[A], ordering: Ordering[A], canSort: CanSort[E#Repr]): BiDataPipelineT[F, Er, A, E] =
    new SortedPipelineT[A, F, Er, E](
      `this`,
      F,
      canSort.asInstanceOf[CanSort[E#Repr]]
    )

  /**
    * Orders elements of the [[BiDataPipelineT]] using criteria [[B]]
    * having an [[Ordering]] defined for type [[B]]
    *
    * @return - the same pipeline sorted
    **/
  def sortBy[B: Ordering](f: A => B)(
      implicit A: ClassTag[A],
      F: Monad[F],
      canSort: CanSort[E#Repr]
  ): BiDataPipelineT[F, Er, A, E] =
    new SortedPipelineT[A, F, Er, E](
      `this`,
      F,
      canSort.asInstanceOf[CanSort[E#Repr]]
    )(Ordering.by(f), A)

  /**
    * Allows to transform [[E]] environment internal data representation.
    * For instance, using [[mapRepr]] you can call [[E#Repr]] specific functions
    * (.via on Akka Stream, combineByKey on RDD, etc.)
    **/
  def mapRepr[B: ClassTag](f: E#Repr[A] => E#Repr[B])(
      implicit F: Monad[F],
      E: E,
      run: E#Run[F],
      A: ClassTag[A]
  ): BiDataPipelineT[F, Er, B, E] =
    MapReprPipeline.make[F, Er, A, B, E](`this`, E)(
      widen(f)(E),
      F,
      widen(run)(E)
    )

  def tapRepr[U: ClassTag](f: E#Repr[A] => U)(
      implicit F: Monad[F],
      E: E,
      run: E#Run[F],
      A: ClassTag[A]
  ): BiDataPipelineT[F, Er, A, E] = mapRepr[A] { repr =>
    f(repr)
    repr
  }

  /**
    * Allows to transform [[E]] environment internal data representation within [[F]] context.
    * For instance, using [[mapRepr]] you can call [[E#Repr]] specific functions
    * (.via on Akka Stream, combineByKey on RDD, etc.)
    **/
  def mapReprF[B: ClassTag](f: E#Repr[A] => F[E#Repr[B]])(
      implicit F: Monad[F],
      E: E,
      run: E#Run[F],
      A: ClassTag[A],
      canFlatMap: CanFlatMap[E]
  ): BiDataPipelineT[F, Er, B, E] =
    MapReprFPipeline.make[F, Er, A, B, E](`this`, E)(
      widenF(f)(E),
      F,
      widen(run)(E)
    )

  /**
    * Monad.flatMap
    **/
  def flatMap[B: ClassTag](f: A => BiDataPipelineT[F, Er, B, E])(
      implicit F: Monad[F],
      E: E,
      run: E#Run[F],
      A: ClassTag[A],
      canFlatMap: CanFlatMap[E],
      ctg: ClassTag[E#Repr[B]]
  ): BiDataPipelineT[F, Er, B, E] =
    `this`.mapReprF[B] { repr =>
      F.map(
        E.TraverseRepr.traverse(repr.asInstanceOf[E.Repr[A]])(a => f(a).evalFunc[B](E)(widen(run)))(
          ctg.asInstanceOf[ClassTag[E.Repr[B]]],
          widen(run)
        )
      )(reprF => canFlatMap.flatten(reprF.asInstanceOf[E#Repr[E#Repr[B]]]))
    }

  /**
    * Forces evaluation of [[E]] internal representation so that further transformations won't be chained with previous ones.
    * Examples:
    * - for sequential pipeline it leads to intermediate collection allocation
    * - for Akka / Spark pipelines it's not such necessary
    * */
  def memoize()(implicit F: Monad[F], E: E, run: E#Run[F], A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    EvaluatedSource.make[F, Er, A, E](evalRepr.asInstanceOf[F[E#Repr[A]]] /* The cast is not redundant! Do not trust IDEA =) */, F)

  /**
    * Special case of [[distinctBy]]
    * Guarantees that each element of pipeline is unique
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    * implemented for type [[A]]
    *
    * @return - pipeline with only unique elements
    **/
  def distinct(implicit canDistinct: CanDistinct[E#Repr], A: ClassTag[A], F: Monad[F], E: E, run: E#Run[F]): BiDataPipelineT[F, Er, A, E] =
    `this`.mapRepr(canDistinct.distinct)

  /**
    * Guarantees that each element of pipeline is unique
    * according to the given criteria
    *
    * CONTRACT: the caller is responsible for correct {{{equals}}}
    *
    * @return - pipeline with only unique elements
    **/
  def distinctBy[K: ClassTag](
      f: A => K
  )(implicit canDistinctBy: CanDistinctBy[E#Repr], F: Monad[F], A: ClassTag[A], E: E, run: E#Run[F]): BiDataPipelineT[F, Er, A, E] =
    `this`.mapRepr(canDistinctBy.distinctBy(_)(f))

  def grouped(
      size: Int
  )(implicit canGrouped: CanGrouped[E#Repr], F: Monad[F], A: ClassTag[A], E: E, run: E#Run[F]): BiDataPipelineT[F, Er, Iterable[A], E] =
    `this`.mapRepr(canGrouped.grouped(_, size))

  def batched(
      parts: Int
  )(implicit canBatched: CanBatched[E#Repr], F: Monad[F], A: ClassTag[A], E: E, run: E#Run[F]): BiDataPipelineT[F, Er, Iterable[A], E] =
    `this`.mapRepr(canBatched.batched(_, parts))

  private def widen(run: E#Run[F])(implicit E: E): E.Run[F] =
    run.asInstanceOf[E.Run[F]]

  private def widen(f: Functor[E#Result])(
      implicit E: E
  ): Functor[E.Result] = f.asInstanceOf[Functor[E.Result]]

  private def widen[x](repr: E#Repr[x])(implicit E: E): E.Repr[x] =
    repr.asInstanceOf[E.Repr[x]]

  private def widen[x, y](
      f: E#Repr[x] => E#Repr[y]
  )(implicit E: E): E.Repr[x] => E.Repr[y] =
    f.asInstanceOf[E.Repr[x] => E.Repr[y]]

  private def widenF[x, y](
      f: E#Repr[x] => F[E#Repr[y]]
  )(implicit E: E): E.Repr[x] => F[E.Repr[y]] =
    f.asInstanceOf[E.Repr[x] => F[E.Repr[y]]]
}
