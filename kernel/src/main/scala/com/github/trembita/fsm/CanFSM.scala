package com.github.trembita.fsm

import java.util.concurrent.atomic.AtomicReference
import cats.Id
import cats.effect.Sync
import cats.effect.concurrent.Ref
import com.github.trembita._
import cats.implicits._
import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Unable to run Finite state machine with context ${F} in ${E}.
    In most cases it means that ${E} does not provide an efficient implementation
    for stateful transformations
  """)
trait CanFSM[F[_], E <: Environment] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: DataPipelineT[F, A, E]
  )(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): DataPipelineT[F, B, E]
}

object CanFSM {
  implicit val fsmIdForSequential: CanFSM[Id, Sequential] =
    new FromIdSeq

  implicit val fsmIdForParallel: CanFSM[Id, Parallel] =
    new FromIdParallel

  implicit def fsmSyncForSequential[F[_]](
      implicit F: Sync[F]
  ): CanFSM[F, Sequential] =
    new FromSyncSequential[F]

  implicit def fsmSyncForParallel[F[_]](
      implicit F: Sync[F]
  ): CanFSM[F, Parallel] =
    new FromSyncParallel[F]
}

class FromIdSeq extends CanFSM[Id, Sequential] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: DataPipelineT[Id, A, Sequential]
  )(initial: InitialState[N, D, Id])(
      fsmF: FSM.Empty[Id, N, D, A, B] => FSM.Func[Id, N, D, A, B]
  ): DataPipelineT[Id, B, Sequential] = {
    val stateF                                = fsmF(new FSM.Empty)
    var stateOpt: Option[FSM.State[N, D, Id]] = None
    pipeline map { elem: A =>
      val elemF: Iterable[B] = {
        val currState = stateOpt match {
          case None =>
            initial match {
              case InitialState.Pure(s) => s
              case InitialState.FromFirstElement(
                  f: (A => FSM.State[N, D, Id])
                  ) =>
                f(elem)
            }
          case Some(s) => s
        }
        val (newState, elems) = stateF(currState)(elem)
        stateOpt = Some(newState)
        elems
      }
      elemF
    } flatMap { vs =>
      vs
    }
  }
}

class FromIdParallel extends CanFSM[Id, Parallel] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: DataPipelineT[Id, A, Parallel]
  )(initial: InitialState[N, D, Id])(
      fsmF: FSM.Empty[Id, N, D, A, B] => FSM.Func[Id, N, D, A, B]
  ): DataPipelineT[Id, B, Parallel] = {
    val stateF = fsmF(new FSM.Empty)
    val stateOpt: AtomicReference[Option[FSM.State[N, D, Id]]] =
      new AtomicReference(None)
    pipeline map { elem: A =>
      val elemF: Iterable[B] = {
        val currState = stateOpt.get() match {
          case None =>
            initial match {
              case InitialState.Pure(s) => s
              case InitialState.FromFirstElement(
                  f: (A => FSM.State[N, D, Id])
                  ) =>
                f(elem)
            }
          case Some(s) => s
        }
        val (newState, elems) = stateF(currState)(elem)
        stateOpt.set(Some(newState))
        elems
      }
      elemF
    } flatMap { vs =>
      vs
    }
  }
}

class FromSyncSequential[F[_]](implicit F: Sync[F]) extends CanFSM[F, Sequential] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: DataPipelineT[F, A, Sequential]
  )(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): DataPipelineT[F, B, Sequential] = {
    val stateF    = fsmF(new FSM.Empty)
    val stateOptF = Ref.unsafe[F, Option[FSM.State[N, D, F]]](None)
    pipeline mapM { elem: A =>
      val elemF: F[Iterable[B]] =
        stateOptF.get.flatMap { stateOpt =>
          val currState = stateOpt match {
            case None =>
              initial match {
                case InitialState.Pure(s) => s
                case InitialState.FromFirstElement(
                    f: (A => FSM.State[N, D, F])
                    ) =>
                  f(elem)
              }
            case Some(s) => s
          }
          stateF(currState)(elem).flatMap {
            case (newState, b) =>
              stateOptF.set(Some(newState)).as(b)
          }
        }
      elemF
    } flatMap { vs =>
      vs
    }
  }
}

class FromSyncParallel[F[_]](implicit F: Sync[F]) extends CanFSM[F, Parallel] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: DataPipelineT[F, A, Parallel]
  )(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): DataPipelineT[F, B, Parallel] = {
    val stateF    = fsmF(new FSM.Empty)
    val stateOptF = Ref.unsafe[F, Option[FSM.State[N, D, F]]](None)
    pipeline mapM { elem: A =>
      val elemF: F[Iterable[B]] =
        stateOptF.get.flatMap { stateOpt =>
          val currState = stateOpt match {
            case None =>
              initial match {
                case InitialState.Pure(s) => s
                case InitialState.FromFirstElement(
                    f: (A => FSM.State[N, D, F])
                    ) =>
                  f(elem)
              }
            case Some(s) => s
          }
          stateF(currState)(elem).flatMap {
            case (newState, b) =>
              stateOptF.set(Some(newState)).as(b)
          }
        }
      elemF
    } flatMap { vs =>
      vs
    }
  }
}
