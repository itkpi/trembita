package com.github.trembita.operations

import cats._
import cats.implicits._
import com.github.trembita.internal._
import com.github.trembita.{DataPipelineT, Environment}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

trait EnvironmentDependentOps[F[_], A, E <: Environment] extends Any {
  def `this`: DataPipelineT[F, A, E]

  /**
    * Evaluates pipeline elements collecting them into Vector
    * */
  def eval(implicit F: Functor[F], E: E, run: E#Run[F], toVector: CanToVector[E#Repr]): F[toVector.Result[Vector[A]]] =
    `this`
      .evalFunc[A](E)(widen(run)(E))
      .map(repr => toVector(repr))

  /**
    * Evaluates pipeline elements applying side-effect on each of them
    * */
  def foreach(
      f: A => Unit
  )(implicit E: E, run: E#Run[F], F: Functor[F]): F[E.Result[Unit]] =
    `this`
      .evalFunc[A](E)(widen(run)(E))
      .map(E.foreach(_)(f))

  /**
    * Evaluates pipeline elements applying monadic side-effect on each of them
    * */
  def foreachF(
      f: A => F[Unit]
  )(implicit E: E, run: E#Run[F], F: Monad[F]): F[Unit] =
    `this`
      .evalFunc[A](E)(widen(run)(E))
      .flatMap(E.foreachF(_)(f)(widen(run)(E), F))

  /**
    * Evaluates environment-specific data representation
    * */
  def evalRepr(implicit E: E, run: E#Run[F]): F[E.Repr[A]] =
    `this`.evalFunc[A](E)(widen(run)(E))

  private def mapEvaledRepr[B](
      f: E#Repr[A] => B
  )(implicit E: E, run: E#Run[F], F: Functor[F]): F[B] =
    evalRepr.map(f)

  /**
    * Reduces [[DataPipelineT]]
    * into single element
    * having a [[Monoid]] defined for type [[A]]
    *
    * @param M - monoid for pipeline's elements
    * @return - combined elements
    **/
  def combineAll(implicit M: Monoid[A],
                 E: E,
                 run: E#Run[F],
                 F: Functor[F],
                 canFold: CanFold[E#Repr],
                 A: ClassTag[A]): F[canFold.Result[A]] =
    mapEvaledRepr(canFold.reduce(_)(M.combine))

  /**
    * UNSAFE version of [[reduceOpt]]
    *
    * Reduces [[DataPipelineT]]
    * into single element [[A]]
    * using reducing function
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduce(
      f: (A, A) => A
  )(implicit E: E, run: E#Run[F], F: Functor[F], canFold: CanFold[E#Repr], A: ClassTag[A]): F[canFold.Result[A]] =
    mapEvaledRepr(canFold.reduce(_)(f))

  /**
    * SAFE version of [[combineAll]]
    * handling empty [[DataPipelineT]]
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduceOpt(
      f: (A, A) => A
  )(implicit E: E, run: E#Run[F], F: Functor[F], canFold: CanFold[E#Repr], A: ClassTag[A]): F[canFold.Result[Option[A]]] =
    mapEvaledRepr(canFold.reduceOpt(_)(f))

  /**
    * Left oriented fold function
    * returning a single instance of [[C]]
    *
    * @tparam C - type of the combiner
    * @param zero - initial combiner
    * @param f    - function for 'adding' a value of type [[A]] to the combiner [[C]]
    * @return - a single instance of the combiner [[C]]
    **/
  def foldLeft[C: ClassTag](zero: C)(f: (C, A) => C)(
      implicit E: E,
      run: E#Run[F],
      F: Functor[F],
      canFold: CanFold[E#Repr],
      A: ClassTag[A]
  ): F[canFold.Result[C]] =
    mapEvaledRepr(canFold.foldLeft(_)(zero)(f))

  /**
    * @return - size of the [[DataPipelineT]]
    **/
  def size(implicit F: Functor[F], E: E, run: E#Run[F], Result: Functor[E#Result], hasSize: HasSize[E#Repr]): F[hasSize.Result[Int]] =
    mapEvaledRepr(hasSize.size(_))

  /**
    * Takes only first N elements of the pipeline
    *
    * @param n - number of elements to take
    * @return - first N elements
    **/
  def take(n: Int)(implicit F: Monad[F], canTake: CanTake[E#Repr], A: ClassTag[A], E: E, Run: E#Run[F]): DataPipelineT[F, A, E] =
    new SeqSource[F, A, E](F) {
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
  def drop(n: Int)(implicit F: Monad[F], canDrop: CanDrop[E#Repr], A: ClassTag[A], E: E, Run: E#Run[F]): DataPipelineT[F, A, E] =
    new SeqSource[F, A, E](F) {
      protected[trembita] def evalFunc[B >: A](
          ex0: E
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(`this`.evalFunc[B](E)(widen(Run)(E)))(canDrop.drop(_, n))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  def slice(from: Int,
            to: Int)(implicit F: Monad[F], canSlice: CanSlice[E#Repr], A: ClassTag[A], E: E, Run: E#Run[F]): DataPipelineT[F, A, E] =
    new SeqSource[F, A, E](F) {
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
  ): DataPipelineT[F, A, Ex2] =
    BridgePipelineT.make[F, A, E, Ex2](`this`, E, F)(
      A,
      widen(run1)(E),
      InjectTaggedK
        .fromId[F, E#Repr, Ex2#Repr](injectK)
        .asInstanceOf[InjectTaggedK[E.Repr, λ[α => F[Ex2#Repr[α]]]]]
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
      injectK: InjectTaggedK[E#Repr, λ[α => F[Ex2#Repr[α]]]]
  ): DataPipelineT[F, A, Ex2] =
    BridgePipelineT.make[F, A, E, Ex2](`this`, E, F)(
      A,
      widen(run1)(E),
      injectK.asInstanceOf[InjectTaggedK[E.Repr, λ[α => F[Ex2#Repr[α]]]]]
    )

  /**
    * Allows to change pipeline evaluation context
    * (for instance from IO to scala.concurrent.Future)
    * */
  def mapK[G[_]](arrow: F ~> G)(implicit G: Monad[G], E: E, run0: E#Run[F], A: ClassTag[A]): DataPipelineT[G, A, E] =
    MapKPipelineT.make[F, G, A, E](`this`, E, arrow, G)(A, widen(run0)(E))

  /**
    * Orders elements of the [[DataPipelineT]]
    * having an [[Ordering]] defined for type [[A]]
    *
    * @return - the same pipeline sorted
    **/
  def sorted(implicit F: Monad[F], A: ClassTag[A], ordering: Ordering[A], canSort: CanSort[E#Repr]): DataPipelineT[F, A, E] =
    new SortedPipelineT[A, F, E](
      `this`,
      F,
      canSort.asInstanceOf[CanSort[E#Repr]]
    )

  /**
    * Orders elements of the [[DataPipelineT]] using criteria [[B]]
    * having an [[Ordering]] defined for type [[B]]
    *
    * @return - the same pipeline sorted
    **/
  def sortBy[B: Ordering](f: A => B)(
      implicit A: ClassTag[A],
      F: Monad[F],
      canSort: CanSort[E#Repr]
  ): DataPipelineT[F, A, E] =
    new SortedPipelineT[A, F, E](
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
  ): DataPipelineT[F, B, E] =
    MapReprPipeline.make[F, A, B, E](`this`, E)(
      widen(f)(E),
      F,
      widen(run)(E)
    )

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
  ): DataPipelineT[F, B, E] =
    MapReprFPipeline.make[F, A, B, E](`this`, E)(
      widenF(f)(E),
      F,
      widen(run)(E)
    )

  /**
    * Monad.flatMap
    **/
  def flatMap[B: ClassTag](f: A => DataPipelineT[F, B, E])(
      implicit F: Monad[F],
      E: E,
      run: E#Run[F],
      A: ClassTag[A],
      canFlatMap: CanFlatMap[E],
      ctg: ClassTag[E#Repr[B]]
  ): DataPipelineT[F, B, E] =
    `this`.mapReprF[B] { repr =>
      F.map(
        E.TraverseRepr.traverse(repr.asInstanceOf[E.Repr[A]])(a => f(a).evalFunc[B](E)(widen(run)))(
          ctg.asInstanceOf[ClassTag[E.Repr[B]]],
          widen(run)
        )
      )(reprF => canFlatMap.flatten(reprF.asInstanceOf[E#Repr[E#Repr[B]]]))
    }

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
