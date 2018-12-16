package com.github.trembita.operations

import cats._
import cats.implicits._
import com.github.trembita.internal._
import com.github.trembita.{
  ApplicativeFlatMap,
  CanFold,
  DataPipelineT,
  Environment
}

import scala.language.higherKinds
import scala.reflect.ClassTag

protected[trembita] class EnvironmentDependentOps[F[_], A, Ex <: Environment](
  val Ex: Ex
)(self: DataPipelineT[F, A, Ex])
    extends Serializable {

  def eval(implicit F: Functor[F], run: Ex.Run[F]): F[Ex.Result[Vector[A]]] =
    self
      .evalFunc[A](Ex)
      .map(repr => Ex.toVector(repr.asInstanceOf[Ex.Repr[A]]))

  def foreach(f: A => Unit)(implicit run: Ex.Run[F],
                            F: Functor[F]): F[Ex.Result[Unit]] =
    self
      .evalFunc[A](Ex)
      .map(Ex.foreach(_)(f))

  def foreachF(f: A => F[Unit])(implicit run: Ex.Run[F], F: Monad[F]): F[Unit] =
    self
      .evalFunc[A](Ex)
      .flatMap(Ex.foreachF(_)(f))

  def evalRepr(implicit run: Ex.Run[F]): F[Ex.Repr[A]] =
    self.evalFunc[A](Ex)

  private def mapEvaledRepr[B](f: Ex.Repr[A] => B)(implicit Run: Ex.Run[F],
                                                   F: Functor[F]): F[B] =
    evalRepr.map(f)

  private def mapEvaled[B](f: Vector[A] => B)(
    implicit Run: Ex.Run[F],
    F: Functor[F],
    Result: Functor[Ex.Result]
  ): F[Ex.Result[B]] =
    eval.map(Result.map(_)(f))

  /**
    * Reduces [[DataPipelineT]]
    * into single element
    * having a [[Monoid]] defined for type [[A]]
    *
    * @param M - monoid for pipeline's elements
    * @return - combined elements
    **/
  def reduce(implicit M: Monoid[A],
             run: Ex.Run[F],
             F: Functor[F],
             canFold: CanFold[Ex.Repr],
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
  def reduce(f: (A, A) => A)(implicit run: Ex.Run[F],
                             F: Functor[F],
                             canFold: CanFold[Ex.Repr],
                             A: ClassTag[A]): F[canFold.Result[A]] =
    mapEvaledRepr(canFold.reduce(_)(f))

  /**
    * SAFE version of [[reduce]]
    * handling empty [[DataPipelineT]]
    *
    * @param f - reducing function
    * @return - combined elements
    **/
  def reduceOpt(f: (A, A) => A)(implicit run: Ex.Run[F],
                                F: Functor[F],
                                canFold: CanFold[Ex.Repr],
                                A: ClassTag[A]): F[canFold.Result[Option[A]]] =
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
    implicit run: Ex.Run[F],
    F: Functor[F],
    canFold: CanFold[Ex.Repr],
    A: ClassTag[A]
  ): F[canFold.Result[C]] =
    mapEvaledRepr(canFold.foldLeft(_)(zero)(f))

  /**
    * @return - size of the [[DataPipelineT]]
    **/
  def size(implicit F: Functor[F],
           run: Ex.Run[F],
           Result: Functor[Ex.Result],
           hasSize: HasSize[Ex.Repr]): F[hasSize.Result[Int]] =
    mapEvaledRepr(hasSize.size(_))

  /**
    * Takes only first N elements of the pipeline
    *
    * @param n - number of elements to take
    * @return - first N elements
    **/
  def take(n: Int)(implicit F: Monad[F],
                   canTake: CanTake[Ex.Repr],
                   A: ClassTag[A],
                   Run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    new SeqSource[F, A, Ex](F) {
      protected[trembita] def evalFunc[B >: A](
        ex0: Ex
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(self.evalFunc[B](Ex)(Run))(canTake.take(_, n))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  //
  //  /**
  //    * Take all elements of the pipeline
  //    * dropping first N elements
  //    *
  //    * @param n - number of elements to drop
  //    * @return - all elements with first N dropped
  //    **/
  def drop(n: Int)(implicit F: Monad[F],
                   canDrop: CanDrop[Ex.Repr],
                   A: ClassTag[A],
                   Run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    new SeqSource[F, A, Ex](F) {
      protected[trembita] def evalFunc[B >: A](
        ex0: Ex
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(self.evalFunc[B](Ex)(Run))(canDrop.drop(_, n))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  def slice(from: Int, to: Int)(implicit F: Monad[F],
                                canSlice: CanSlice[Ex.Repr],
                                A: ClassTag[A],
                                Run: Ex.Run[F]): DataPipelineT[F, A, Ex] =
    new SeqSource[F, A, Ex](F) {
      protected[trembita] def evalFunc[B >: A](
        ex0: Ex
      )(implicit run: ex0.Run[F]): F[ex0.Repr[B]] =
        F.map(
            self
              .evalFunc[B](Ex)(Run)
          )(canSlice.slice(_, from, to))
          .asInstanceOf[F[ex0.Repr[B]]]
    }

  //
  /**
    * UNSAFE version of [[headOption]]
    *
    * @return - first element of the pipeline
    **/
  def head(implicit run: Ex.Run[F],
           F: Functor[F],
           canTake: CanTake[Ex.Repr],
           Result: Functor[Ex.Result]): F[Ex.Result[A]] =
    headOption.map(_.map(_.get))

  /**
    * @return - {{{Some(firstElement)}}} if pipeline is not empty, [[None]] otherwise
    **/
  def headOption(implicit run: Ex.Run[F],
                 F: Functor[F],
                 canTake: CanTake[Ex.Repr],
                 Result: Functor[Ex.Result]): F[Ex.Result[Option[A]]] =
    mapEvaledRepr(canTake.take(_, 1)).map(Ex.toVector(_).map(_.headOption))

  def to[Ex2 <: Environment](
    implicit run1: Ex.Run[F],
    A: ClassTag[A],
    F: Monad[F],
    injectK: InjectTaggedK[Ex.Repr, Ex2#Repr]
  ): DataPipelineT[F, A, Ex2] =
    BridgePipelineT.make[F, A, Ex, Ex2](self, Ex, F)

  def mapK[G[_]](arrow: F ~> G)(implicit G: Monad[G],
                                run0: Ex.Run[F],
                                A: ClassTag[A]): DataPipelineT[G, A, Ex] =
    MapKPipelineT.make[F, G, A, Ex](self, Ex, arrow, G)

  /**
    * Orders elements of the [[DataPipelineT]]
    * having an [[Ordering]] defined for type [[A]]
    *
    * @return - the same pipeline sorted
    **/
  def sorted(implicit F: Monad[F],
             A: ClassTag[A],
             ordering: Ordering[A],
             canSort: CanSort[Ex.Repr]): DataPipelineT[F, A, Ex] =
    new SortedPipelineT[A, F, Ex](
      self,
      F,
      canSort.asInstanceOf[CanSort[Ex#Repr]]
    )

  def sortBy[B: Ordering](f: A => B)(
    implicit A: ClassTag[A],
    F: Monad[F],
    canSort: CanSort[Ex.Repr]
  ): DataPipelineT[F, A, Ex] =
    new SortedPipelineT[A, F, Ex](
      self,
      F,
      canSort.asInstanceOf[CanSort[Ex#Repr]]
    )(Ordering.by(f), A)

  def mapRepr[B: ClassTag](f: Ex.Repr[A] => Ex.Repr[B])(
    implicit F: Monad[F],
    run: Ex.Run[F],
    A: ClassTag[A]
  ): DataPipelineT[F, B, Ex] =
    MapReprPipeline.make[F, A, B, Ex](self, Ex)(f, F, run)
}
