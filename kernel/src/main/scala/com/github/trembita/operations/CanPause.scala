package com.github.trembita.operations

import cats.effect.{Sync, Timer}
import com.github.trembita._

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
trait CanPause[F[_], E <: Environment] extends Serializable {
  def pausedWith[A: ClassTag](pipeline: DataPipelineT[F, A, E])(getPause: A => FiniteDuration): DataPipelineT[F, A, E]
  def paused[A: ClassTag](pipeline: DataPipelineT[F, A, E])(pause: FiniteDuration): DataPipelineT[F, A, E] =
    pausedWith(pipeline)(_ => pause)
}

trait LowPriorityPause extends Serializable  {
  implicit def canPauseSequential[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause[F, Sequential] = new CanPause[F, Sequential] {
    def pausedWith[A: ClassTag](pipeline: DataPipelineT[F, A, Sequential])(
        getPause: A => FiniteDuration
    ): DataPipelineT[F, A, Sequential] = pipeline.mapMImpl[A, A](a => timer.sleep(getPause(a)) *> F.pure(a))
  }

  implicit def canPauseParallel[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause[F, Parallel] = new CanPause[F, Parallel] {
    def pausedWith[A: ClassTag](pipeline: DataPipelineT[F, A, Parallel])(
        getPause: A => FiniteDuration
    ): DataPipelineT[F, A, Parallel] = pipeline.mapMImpl[A, A](a => timer.sleep(getPause(a)) *> F.pure(a))
  }
}

object CanPause extends LowPriorityPause {
  def apply[F[_], E <: Environment](implicit ev: F CanPause E): F CanPause E = ev
}

@implicitNotFound("""
    Unable to pause elements evaluation (with pause based on 2 pipeline elements) within ${E} using ${F}.
    Please ensure right implicits in scope or provide your own implementation
    """)
trait CanPause2[F[_], E <: Environment] {
  def pausedWith[A: ClassTag](pipeline: DataPipelineT[F, A, E])(getPause: (A, A) => FiniteDuration): DataPipelineT[F, A, E]
}

object CanPause2 {
  def apply[F[_], E <: Environment](implicit ev: F CanPause2 E): F CanPause2 E = ev

  implicit def canPauseSequential[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause2[F, Sequential] = new CanPause2[F, Sequential] {
    override def pausedWith[A: ClassTag](
        pipeline: DataPipelineT[F, A, Sequential]
    )(
        getPause: (A, A) => FiniteDuration
    ): DataPipelineT[F, A, Sequential] =
      pipeline.mapReprF(_.foldLeft(F.pure(Vector.newBuilder[A]) -> Option.empty[A]) {
        case ((accF, None), a)          => (accF.map(_ += a), Some(a))
        case ((accF, Some(prev)), curr) => (timer.sleep(getPause(prev, curr)) *> accF.map(_ += curr), Some(curr))
      }._1.map(_.result()))
  }

  implicit def canPauseParallel[F[_]](implicit F: Sync[F], timer: Timer[F]): CanPause2[F, Parallel] = new CanPause2[F, Parallel] {
    override def pausedWith[A: ClassTag](
        pipeline: DataPipelineT[F, A, Parallel]
    )(
        getPause: (A, A) => FiniteDuration
    ): DataPipelineT[F, A, Parallel] =
      pipeline.mapReprF(_.foldLeft(F.pure(ParVector.newBuilder[A]) -> Option.empty[A]) {
        case ((accF, None), a)          => (accF.map(_ += a), Some(a))
        case ((accF, Some(prev)), curr) => (timer.sleep(getPause(prev, curr)) *> accF.map(_ += curr), Some(curr))
      }._1.map(_.result()))
  }
}
