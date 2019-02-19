package trembita.fsm

import java.util.concurrent.atomic.AtomicReference
import cats.Id
import cats.effect.Sync
import cats.effect.concurrent.Ref
import trembita._
import cats.implicits._
import scala.annotation.implicitNotFound
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    Unable to run Finite state machine with context ${F} in ${E}.
    In most cases it means that ${E} does not provide an efficient implementation
    for stateful transformations
  """)
trait CanFSM[F[_], Er, E <: Environment] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: BiDataPipelineT[F, Er, A, E]
  )(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): BiDataPipelineT[F, Er, B, E]
}

object CanFSM {
  implicit val fsmIdForSequential: CanFSM[Id, Nothing, Sequential] =
    new FromIdSeq

  implicit val fsmIdForParallel: CanFSM[Id, Nothing, Parallel] =
    new FromIdParallel

  implicit def fsmSyncForSequential[F[_]](
      implicit F: Sync[F]
  ): CanFSM[F, Throwable, Sequential] =
    new FromSyncSequential[F]

  implicit def fsmSyncForParallel[F[_]](
      implicit F: Sync[F]
  ): CanFSM[F, Throwable, Parallel] =
    new FromSyncParallel[F]
}

class FromIdSeq extends CanFSM[Id, Nothing, Sequential] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: BiDataPipelineT[Id, Nothing, A, Sequential]
  )(initial: InitialState[N, D, Id])(
      fsmF: FSM.Empty[Id, N, D, A, B] => FSM.Func[Id, N, D, A, B]
  ): BiDataPipelineT[Id, Nothing, B, Sequential] = {
    val stateF                                = fsmF(new FSM.Empty)
    var stateOpt: Option[FSM.State[N, D, Id]] = None
    pipeline map { elem: A =>
      val elemF: Iterable[B] = {
        val currState = stateOpt match {
          case None =>
            initial match {
              case InitialState.Pure(s) => s
              case InitialState.FromFirstElement(
                  f: (A => FSM.State[N, D, Id]) @unchecked
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
    } mapConcat { vs =>
      vs
    }
  }
}

class FromIdParallel extends CanFSM[Id, Nothing, Parallel] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: BiDataPipelineT[Id, Nothing, A, Parallel]
  )(initial: InitialState[N, D, Id])(
      fsmF: FSM.Empty[Id, N, D, A, B] => FSM.Func[Id, N, D, A, B]
  ): BiDataPipelineT[Id, Nothing, B, Parallel] = {
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
                  f: (A => FSM.State[N, D, Id]) @unchecked
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
    } mapConcat { vs =>
      vs
    }
  }
}

class FromSyncSequential[F[_]](implicit F: Sync[F]) extends CanFSM[F, Throwable, Sequential] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: BiDataPipelineT[F, Throwable, A, Sequential]
  )(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): BiDataPipelineT[F, Throwable, B, Sequential] = {
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
                    f: (A => FSM.State[N, D, F]) @unchecked
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
    } mapConcat { vs =>
      vs
    }
  }
}

class FromSyncParallel[F[_]](implicit F: Sync[F]) extends CanFSM[F, Throwable, Parallel] {
  def fsm[A: ClassTag, N, D, B: ClassTag](
      pipeline: BiDataPipelineT[F, Throwable, A, Parallel]
  )(initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
  ): BiDataPipelineT[F, Throwable, B, Parallel] = {
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
                    f: (A => FSM.State[N, D, F]) @unchecked
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
    } mapConcat { vs =>
      vs
    }
  }
}
