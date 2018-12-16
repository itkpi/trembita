package com.github.trembita

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import com.github.trembita.operations.{LiftPipeline, MagnetF}
import scala.language.higherKinds
import scala.reflect.ClassTag

package object fsm {

  implicit class StatefulOps[A, F[_], Ex <: Environment](
    val self: DataPipelineT[F, A, Ex]
  ) extends AnyVal {

    /**
      * Map [[DataPipelineT]] elements
      * into an instance of type [[B]]
      * according to some state
      *
      * @tparam N - named state
      * @tparam D - state data type
      * @tparam B - resulting type
      * @param initial - initial state
      * @param fsmF    - see DSL for providing a FSM
      * @return - mapped pipeline
      **/
    def fsm[N, D, B: ClassTag](initial: InitialState[N, D, F])(
      fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
    )(implicit F: Sync[F],
      ev: (A => F[Iterable[B]]) => MagnetF[F, A, Iterable[B], Ex],
      liftPipeline: LiftPipeline[F, Ex]): DataPipelineT[F, B, Ex] = {
      val stateF = fsmF(new FSM.Empty)
      val stateOptF = Ref.unsafe[F, Option[FSM.State[N, D, F]]](None)
      self mapM { elem: A =>
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
      } flatMapImpl { vs =>
        vs
      }
    }
  }
}
