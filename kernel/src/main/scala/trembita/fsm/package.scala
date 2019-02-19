package trembita

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import trembita.operations.{LiftPipeline, MagnetF}
import scala.language.higherKinds
import scala.reflect.ClassTag

package object fsm {

  implicit class StatefulOps[A, F[_], Er, E <: Environment](
      val self: BiDataPipelineT[F, Er, A, E]
  ) extends AnyVal {

    /**
      * Map [[BiDataPipelineT]] elements
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
    )(implicit canFSM: CanFSM[F, Er, E], A: ClassTag[A]): BiDataPipelineT[F, Er, B, E] =
      canFSM.fsm[A, N, D, B](self)(initial)(fsmF)
  }
}
