package com.datarootlabs.trembita

import cats.Id
import cats.implicits._
import cats.effect._
import com.datarootlabs.trembita.internal.StrictSource


package object fsm {
  implicit class StatefulOps[A](val self: DataPipeline[A]) extends AnyVal {

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
    def statefulMap[N, D, B](initial: InitialState[N, D],
                                   result: FSM.Result[B, FSM.State[N, D]])
                                  (fsmF: FSM.Empty[Id, N, D, A, B] ⇒ FSM.Func[Id, N, D, A, B]): DataPipeline[result.Out] =
      new StrictSource({
        val iter = self.iterator
        if (!iter.hasNext) Vector.empty
        else {
          val stateF = fsmF(new FSM.Empty)
          val builder = Vector.newBuilder[result.Out]
          var state: FSM.State[N, D] = initial match {
            case InitialState.Pure(s)                                    ⇒ s
            case InitialState.FromFirstElement(f: (A ⇒ FSM.State[N, D])) ⇒ f(iter.next())
          }
          while (iter.hasNext) {
            val elem = iter.next()
            val (newState, b) = stateF(state)(elem)
            state = newState
            builder += result(state, b)
          }
          builder.result()
        }
      })

    /**
      * FlatMap [[DataPipeline]] elements
      * into instances of type [[B]]
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
    def statefulFlatMap[N, D, B](initial: InitialState[N, D],
                                 result: FSM.Result[B, FSM.State[N, D]])
                                (fsmF: FSM.Empty[Id, N, D, A, Iterable[B]] ⇒ FSM.Func[Id, N, D, A, Iterable[B]])
    : DataPipeline[result.Out] =
      new StrictSource({
        val iter = self.iterator
        if (!iter.hasNext) Vector.empty
        else {
          val stateF = fsmF(new FSM.Empty)
          val builder = Vector.newBuilder[result.Out]
          var state: FSM.State[N, D] = initial match {
            case InitialState.Pure(s)                                    ⇒ s
            case InitialState.FromFirstElement(f: (A ⇒ FSM.State[N, D])) ⇒ f(iter.next())
          }
          while (iter.hasNext) {
            val elem = iter.next()
            val (newState, bs) = stateF(state)(elem)
            state = newState
            builder ++= bs.map(result(state, _))
          }
          builder.result()
        }
      })
  }
}
