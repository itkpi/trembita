package com.datarootlabs.trembita

import cats._
import cats.implicits._
import cats.effect._
import com.datarootlabs.trembita.internal.StrictSource
import com.datarootlabs.trembita.mutable.StateEff


package object fsm {

  implicit class StatefulOps[A, F[_], T <: Finiteness, Ex <: Execution](val self: DataPipeline[A, F, T, Ex]) extends AnyVal {

    /**
      * Map [[DataPipeline]] elements
      * into an instance of type [[B]]
      * according to some state
      *
      * @tparam N - named state
      * @tparam D - state data type
      * @tparam B - resulting type
      * @param initial - initial state
      * @param result  - what to produce as a result of transformations
      * @param fsmF    - see DSL for providing a FSM
      * @return - mapped pipeline
      **/
    def fsm[N, D, B](initial: InitialState[N, D, F],
                     result: FSM.Result[B, FSM.State[N, D, F]])
                    (fsmF: FSM.Empty[F, N, D, A, B] ⇒ FSM.Func[F, N, D, A, B])
                    (implicit F: MonadError[F, Throwable]): DataPipeline[result.Out, F, T, Ex] = {
      val stateF = fsmF(new FSM.Empty)
      val stateOptF: StateEff[F, Option[FSM.State[N, D, F]]] = StateEff(None)
      self mapM { elem ⇒
        val elemF: F[Either[Throwable, Iterable[result.Out]]] =
          stateOptF.mutateFlatMapSync { stateOpt ⇒
            val currState = stateOpt match {
              case None    ⇒ initial match {
                case InitialState.Pure(s)                                       ⇒ s
                case InitialState.FromFirstElement(f: (A ⇒ FSM.State[N, D, F])) ⇒ f(elem)
              }
              case Some(s) ⇒ s
            }
            stateF(currState)(elem).map { case (newState, b) ⇒
              val results: Iterable[result.Out] = b.map { bx ⇒
                result(newState, bx)
              }
              Some(newState) → results
            }
          }
        elemF
      } flatCollect {
        case Right(vs) ⇒ vs
      }
    }
  }

}
