package com.github.trembita

import cats._
import com.github.trembita.internal._
import com.github.trembita.operations.LiftPipeline
import scala.annotation.unchecked.uncheckedVariance
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipelineT[F[_], +A, E <: Environment] extends Serializable {

  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipelineT]]
    **/
  protected[trembita] def mapImpl[B: ClassTag](f: A => B)(
      implicit F: Monad[F]
  ): DataPipelineT[F, B, E]

  /**
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{Iterable[B]}}}
    * @return - transformed [[DataPipelineT]]
    **/
  protected[trembita] def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  )(implicit F: Monad[F]): DataPipelineT[F, B, E]

  /**
    * Guarantees that [[DataPipelineT]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipelineT]]
    **/
  protected[trembita] def filterImpl[AA >: A](p: A => Boolean)(implicit F: Monad[F], A: ClassTag[AA]): DataPipelineT[F, AA, E] =
    collectImpl[AA]({ case a if p(a) => a })

  /**
    * Applies a [[PartialFunction]] to the [[DataPipelineT]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipelineT]]
    **/
  protected[trembita] def collectImpl[B: ClassTag](pf: PartialFunction[A, B])(
      implicit F: Monad[F]
  ): DataPipelineT[F, B, E]

  protected[trembita] def mapMImpl[AA >: A, B: ClassTag](f: A => F[B])(implicit F: Monad[F]): DataPipelineT[F, B, E] =
    new MapMonadicPipelineT[F, A, B, E](f, this)(F)

  protected[trembita] def handleErrorImpl[B >: A: ClassTag](f: Throwable => B)(
      implicit F: MonadError[F, Throwable]
  ): DataPipelineT[F, B, E]

  protected[trembita] def handleErrorWithImpl[B >: A: ClassTag](
      f: Throwable => F[B]
  )(implicit F: MonadError[F, Throwable]): DataPipelineT[F, B, E]

  /**
    * Forces evaluation of [[DataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  protected[trembita] def evalFunc[B >: A](Ex: E @uncheckedVariance)(
      implicit run: Ex.Run[F]
  ): F[Ex.Repr[B]]
}

object DataPipelineT {

  /** Implicit conversions */
  implicit def fromIterable[A: ClassTag, F[_], Ex <: Environment](
      iterable: Iterable[A]
  )(implicit liftPipeline: LiftPipeline[F, Ex]): DataPipelineT[F, A, Ex] =
    liftPipeline.liftIterable(iterable)

  implicit def fromArray[A: ClassTag, F[_], Ex <: Environment](
      array: Array[A]
  )(implicit liftPipeline: LiftPipeline[F, Ex]): DataPipelineT[F, A, Ex] =
    liftPipeline.liftIterable(array.toIterable)
}
