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
  implicit def fsmSyncForSequential[F[_]](
      implicit F: Sync[F]
  ): CanFSM[F, Throwable, Sequential] =
    new FromSyncSequential[F]

  implicit def fsmSyncForParallel[F[_]](
      implicit F: Sync[F]
  ): CanFSM[F, Throwable, Parallel] =
    new FromSyncParallel[F]
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
