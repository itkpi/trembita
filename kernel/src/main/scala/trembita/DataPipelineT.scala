package trembita

import trembita.internal._
import trembita.operations.LiftPipeline
import scala.annotation.unchecked.uncheckedVariance
import scala.language.implicitConversions
import scala.reflect.ClassTag
import zio._

/**
  * Generic class representing a lazy pipeline of data
  *
  * @tparam A - type of data
  **/
trait DataPipelineT[+Er, +A, E <: Environment] extends Serializable {

  /**
    * Functor.map
    *
    * @tparam B - resulting data type
    * @param f - transformation function
    * @return - transformed [[DataPipelineT]]
    **/
  @internalAPI
  protected[trembita] def mapImpl[B: ClassTag](f: A => B): DataPipelineT[Er, B, E]

  /**
    *
    * @tparam B - resulting data type
    * @param f - transformation function from [[A]] into {{{Iterable[B]}}}
    * @return - transformed [[DataPipelineT]]
    **/
  @internalAPI
  protected[trembita] def mapConcatImpl[B: ClassTag](
      f: A => Iterable[B]
  ): DataPipelineT[Er, B, E]

  /**
    * Guarantees that [[DataPipelineT]]
    * will consists of elements satisfying given predicate
    *
    * @param p - predicate
    * @return - filtered [[DataPipelineT]]
    **/
  @internalAPI
  protected[trembita] def filterImpl[AA >: A : ClassTag](p: A => Boolean): DataPipelineT[Er, AA, E] =
    collectImpl[AA]({ case a if p(a) => a })

  /**
    * Applies a [[PartialFunction]] to the [[DataPipelineT]]
    *
    * @tparam B - resulting data type
    * @param pf - partial function
    * @return - transformed [[DataPipelineT]]
    **/
  @internalAPI
  protected[trembita] def collectImpl[B: ClassTag](pf: PartialFunction[A, B]): DataPipelineT[Er, B, E]

  @internalAPI
  protected[trembita] def mapMImpl[EE >: Er, AA >: A, B: ClassTag](f: A => IO[EE, B]): DataPipelineT[EE, B, E] =
    new MapMonadicPipelineT[F, A, B, E](f, this)(F)

  @internalAPI
  protected[trembita] def catchAllImpl[B >: A: ClassTag](f: Er => B): DataPipelineT[Nothing, B, E]

  @internalAPI
  protected[trembita] def catchAllWithImpl[B >: A: ClassTag](
      f: Er => UIO[B]
  ): DataPipelineT[Nothing, B, E]

  /**
    * Forces evaluation of [[DataPipelineT]]
    * collecting data into [[Iterable]]
    *
    * @return - collected data
    **/
  @internalAPI
  protected[trembita] def evalFunc[EE >: Er, B >: A](Ex: E @uncheckedVariance): UIO[Ex.Repr[Either[EE, B]]]
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
