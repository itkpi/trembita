package com.datarootlabs.trembita

import com.datarootlabs.trembita.internal.StrictSource


package object fsm {
  implicit class StatefulOps[A](val self: DataPipeline[A]) extends AnyVal {
    def mapWithState[N, D, B](initial: InitialState[N, D],
                              result: FSM.Result[B, FSM.State[N, D]])
                             (fsmF: FSM.Empty[N, D, A, B] ⇒ FSM.Completed[N, D, A, B]): DataPipeline[result.Out] =
      new StrictSource({
        val iter = self.iterator
        if (!iter.hasNext) Vector.empty
        else {
          val stateF = fsmF(new FSM.Empty).complete
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
    def flatMapWithState[N, D, B](initial: InitialState[N, D],
                                  result: FSM.Result[B, FSM.State[N, D]])
                                 (fsmF: FSM.Empty[N, D, A, Iterable[B]] ⇒ FSM.Completed[N, D, A, Iterable[B]])
    : DataPipeline[result.Out] =
      new StrictSource({
        val iter = self.iterator
        if (!iter.hasNext) Vector.empty
        else {
          val stateF = fsmF(new FSM.Empty).complete
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
