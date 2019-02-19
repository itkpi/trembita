package trembita.operations

import cats.effect.{Sync, Timer}
import trembita._

import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import cats.syntax.all._

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag

@implicitNotFound("""
    Unable to pause elements evaluation within ${E} using ${F}.
    Please ensure right implicits in scope or provide your own implementation
    """)
trait CanPause[F[_], Er, E <: Environment] extends Serializable {
  def pausedWith[A: ClassTag](pipeline: BiDataPipelineT[F, Er, A, E])(getPause: A => FiniteDuration): BiDataPipelineT[F, Er, A, E]
  def paused[A: ClassTag](pipeline: BiDataPipelineT[F, Er, A, E])(pause: FiniteDuration): BiDataPipelineT[F, Er, A, E] =
    pausedWith(pipeline)(_ => pause)
}

trait LowPriorityPause extends Serializable {
  implicit def canPauseSequential[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause[F, Throwable, Sequential] =
    new CanPause[F, Throwable, Sequential] {
      def pausedWith[A: ClassTag](pipeline: BiDataPipelineT[F, Throwable, A, Sequential])(
          getPause: A => FiniteDuration
      ): BiDataPipelineT[F, Throwable, A, Sequential] = pipeline.mapMImpl[A, A](a => timer.sleep(getPause(a)) *> F.pure(a))
    }

  implicit def canPauseParallel[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause[F, Throwable, Parallel] =
    new CanPause[F, Throwable, Parallel] {
      def pausedWith[A: ClassTag](pipeline: BiDataPipelineT[F, Throwable, A, Parallel])(
          getPause: A => FiniteDuration
      ): BiDataPipelineT[F, Throwable, A, Parallel] = pipeline.mapMImpl[A, A](a => timer.sleep(getPause(a)) *> F.pure(a))
    }
}

object CanPause extends LowPriorityPause {
  def apply[F[_], Er, E <: Environment](implicit ev: CanPause[F, Er, E]): CanPause[F, Er, E] = ev
}

@implicitNotFound("""
    Unable to pause elements evaluation (with pause based on 2 pipeline elements) within ${E} using ${F}.
    Please ensure right implicits in scope or provide your own implementation
    """)
trait CanPause2[F[_], Er, E <: Environment] {
  def pausedWith[A: ClassTag](pipeline: BiDataPipelineT[F, Er, A, E])(getPause: (A, A) => FiniteDuration): BiDataPipelineT[F, Er, A, E]
}

object CanPause2 {
  def apply[F[_], Er, E <: Environment](implicit ev: CanPause2[F, Er, E]): CanPause2[F, Er, E] = ev

  implicit def canPauseSequential[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause2[F, Throwable, Sequential] =
    new CanPause2[F, Throwable, Sequential] {
      override def pausedWith[A: ClassTag](
          pipeline: BiDataPipelineT[F, Throwable, A, Sequential]
      )(
          getPause: (A, A) => FiniteDuration
      ): BiDataPipelineT[F, Throwable, A, Sequential] =
        pipeline.mapReprF(_.foldLeft(F.pure(Vector.newBuilder[A]) -> Option.empty[A]) {
          case ((accF, None), a)          => (accF.map(_ += a), Some(a))
          case ((accF, Some(prev)), curr) => (timer.sleep(getPause(prev, curr)) *> accF.map(_ += curr), Some(curr))
        }._1.map(_.result()))
    }

  implicit def canPauseParallel[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause2[F, Throwable, Parallel] =
    new CanPause2[F, Throwable, Parallel] {
      override def pausedWith[A: ClassTag](
          pipeline: BiDataPipelineT[F, Throwable, A, Parallel]
      )(
          getPause: (A, A) => FiniteDuration
      ): BiDataPipelineT[F, Throwable, A, Parallel] =
        pipeline.mapReprF(_.foldLeft(F.pure(ParVector.newBuilder[A]) -> Option.empty[A]) {
          case ((accF, None), a)          => (accF.map(_ += a), Some(a))
          case ((accF, Some(prev)), curr) => (timer.sleep(getPause(prev, curr)) *> accF.map(_ += curr), Some(curr))
        }._1.map(_.result()))
    }
}
