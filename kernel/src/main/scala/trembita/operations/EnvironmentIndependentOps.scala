package trembita.operations
import cats.data.Kleisli
import cats.{~>, Monad, MonadError}
import trembita.internal._
import trembita.{operations, BiDataPipelineT, BiPipeT, Environment}

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.reflect.ClassTag

trait EnvironmentIndependentOps[F[_], Er, A, E <: Environment] extends Any {
  def `this`: BiDataPipelineT[F, Er, A, E]

  def flatten[B: ClassTag](implicit ev: A <:< Iterable[B], F: MonadError[F, Er]): BiDataPipelineT[F, Er, B, E] =
    `this`.mapConcatImpl(ev)

  /**
    * Groups the pipeline using given grouping criteria.
    *
    * Returns a [[GroupByPipelineT]] - special implementation of [[BiDataPipelineT]]
    *
    * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
    **/
  def groupByKey[K: ClassTag](f: A => K)(
      implicit canGroupBy: CanGroupBy[E#Repr],
      F: MonadError[F, Er],
      A: ClassTag[A],
      Er: ClassTag[Er]
  ): BiDataPipelineT[F, Er, (K, Iterable[A]), E] = GroupByPipelineT.make[F, Er, K, A, E](f, `this`, F, canGroupBy)

  /**
    * Groups the pipeline using given grouping criteria guaranteeing keys ordering.
    *
    * Returns a [[GroupByPipelineT]] - special implementation of [[BiDataPipelineT]]
    *
    * @return - a data pipeline consisting of pair {{{ (K, Iterable[A]) }}}
    **/
  def groupByKeyOrdered[K: ClassTag: Ordering](f: A => K)(
      implicit canGroupByOrdered: CanGroupByOrdered[E#Repr],
      F: MonadError[F, Er],
      A: ClassTag[A],
      Er: ClassTag[Er]
  ): BiDataPipelineT[F, Er, (K, Iterable[A]), E] = GroupByOrderedPipelineT.make[F, Er, K, A, E](f, `this`, F, canGroupByOrdered)

  def zip[B: ClassTag](
      that: BiDataPipelineT[F, Er, B, E]
  )(implicit A: ClassTag[A], F: MonadError[F, Er], canZip: CanZip[E#Repr], Er: ClassTag[Er]): BiDataPipelineT[F, Er, (A, B), E] =
    new ZipPipelineT[F, Er, A, B, E](`this`, that, canZip)

  def ++(that: BiDataPipelineT[F, Er, A, E])(implicit A: ClassTag[A], F: MonadError[F, Er], Er: ClassTag[Er]): BiDataPipelineT[F, Er, A, E] =
    new ConcatPipelineT[F, Er, A, E](`this`, that)

  def join[B](that: BiDataPipelineT[F, Er, B, E])(on: (A, B) => Boolean)(
      implicit canJoin: CanJoin[E#Repr],
      A: ClassTag[A],
      B: ClassTag[B],
      F: MonadError[F, Er],
      Er: ClassTag[Er]
  ): BiDataPipelineT[F, Er, (A, B), E] =
    new JoinPipelineT[F, Er, A, B, E](`this`, that, on)

  def joinLeft[B](that: BiDataPipelineT[F, Er, B, E])(on: (A, B) => Boolean)(
      implicit canJoin: CanJoin[E#Repr],
      A: ClassTag[A],
      B: ClassTag[B],
      F: MonadError[F, Er],
      Er: ClassTag[Er]
  ): BiDataPipelineT[F, Er, (A, Option[B]), E] =
    new JoinLeftPipelineT[F, Er, A, B, E](`this`, that, on)

  def joinRight[B](that: BiDataPipelineT[F, Er, B, E])(on: (A, B) => Boolean)(
      implicit canJoin: CanJoin[E#Repr],
      A: ClassTag[A],
      B: ClassTag[B],
      F: MonadError[F, Er],
      Er: ClassTag[Er]
  ): BiDataPipelineT[F, Er, (Option[A], B), E] =
    new JoinRightPipelineT[F, Er, A, B, E](`this`, that, on)

  /**
    * Allows to pause elements evaluation with given duration based on single [[A]]
    * */
  def pausedWith(getPause: A => FiniteDuration)(implicit ev: CanPause[F, Er, E], A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    ev.pausedWith(`this`)(getPause)

  /**
    * Allows to pause elements evaluation with given duration based on [[A]]
    * if condition is true
    * */
  def pausedWithIf(cond: Boolean)(getPause: A => FiniteDuration)(implicit ev: CanPause[F, Er, E],
                                                                 A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    if (cond) ev.pausedWith(`this`)(getPause)
    else `this`

  /**
    * Allows to pause elements evaluation with given duration based on 2 elements of type [[A]]
    * */
  def pausedWith2(getPause: (A, A) => FiniteDuration)(implicit ev: CanPause2[F, Er, E], A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    ev.pausedWith(`this`)(getPause)

  /**
    * Allows to pause elements evaluation with given duration based on 2 elements of type [[A]]
    * if condition is true
    * */
  def pausedWith2If(cond: Boolean)(getPause: (A, A) => FiniteDuration)(implicit ev: CanPause2[F, Er, E],
                                                                       A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    if (cond) ev.pausedWith(`this`)(getPause)
    else `this`

  /**
    * Allows to pause elements evaluation with given fixed duration
    * */
  def paused(pause: FiniteDuration)(implicit ev: CanPause[F, Er, E], A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    ev.paused(`this`)(pause)

  /**
    * Allows to pause elements evaluation with given fixed duration
    * if condition is true
    * */
  def pausedIf(cond: Boolean)(pause: FiniteDuration)(implicit ev: CanPause[F, Er, E], A: ClassTag[A]): BiDataPipelineT[F, Er, A, E] =
    if (cond) ev.paused(`this`)(pause)
    else `this`

  /**
    * Allows apply transformations defined as [[Kleisli]] on given pipeline
    * */
  def through[B](pipe: BiPipeT[F, Er, A, B, E]): BiDataPipelineT[F, Er, B, E] =
    pipe.run(`this`)

  /**
    * Like [[groupByKey]] with the following difference:
    *
    * {{{
    *   // groupBy...
    *   List(1, 1, 2, 2, 1, 1).groupBy(identity) === Map(1 -> List(1, 1, 1, 1), 2 -> List(2, 2))
    *   // and spanBy...
    *   List(1, 1, 2, 2, 1, 1).spanBy(identity) === List(1 -> List(1, 1), 2 -> List(2, 2), 1 -> List(1, 1))
    * }}}
    *
    * */
  def spanBy[K](f: A => K)(
      implicit canSpanBy: CanSpanBy[E#Repr],
      A: ClassTag[A],
      K: ClassTag[K],
      F: MonadError[F, Er],
      e: E,
      run: E#Run[F]
  ): BiDataPipelineT[F, Er, (K, Iterable[A]), E] =
    `this`.mapRepr(canSpanBy.spanBy(_)(f))

  def withPrintedPlan(): BiDataPipelineT[F, Er, A, E] = {
    println("[TREMBITA-DEBUG] Plan: " + `this`)
    `this`
  }
}
