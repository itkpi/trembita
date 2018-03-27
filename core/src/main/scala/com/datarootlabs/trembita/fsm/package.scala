package com.datarootlabs.trembita

import com.datarootlabs.trembita.internal.StrictSource


package object fsm {
  implicit class StatefulOps[A](val self: DataPipeline[A]) extends AnyVal {
    def mapWithState[S, B](initial: InitialState[S],
                           result: FSM.Result[B, S])
                          (fsmF: FSM.Empty[S, A, B] ⇒ FSM.Completed[S, A, B]): DataPipeline[result.Out] =
      new StrictSource({
        val iter = self.iterator
        if (!iter.hasNext) Vector.empty
        else {
          val stateF = fsmF(new FSM.Empty).complete
          val builder = Vector.newBuilder[result.Out]
          var state: S = initial match {
            case InitialState.Pure(s)                      ⇒ s
            case InitialState.FromFirstElement(f: (A ⇒ S)) ⇒ f(iter.next())
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
    def flatMapWithState[S, B](initial: InitialState[S],
                               result: FSM.Result[B, S])
                              (fsmF: FSM.Empty[S, A, Iterable[B]] ⇒ FSM.Completed[S, A, Iterable[B]]): DataPipeline[result.Out] =
      new StrictSource({
        val iter = self.iterator
        if (!iter.hasNext) Vector.empty
        else {
          val stateF = fsmF(new FSM.Empty).complete
          val builder = Vector.newBuilder[result.Out]
          var state: S = initial match {
            case InitialState.Pure(s)                      ⇒ s
            case InitialState.FromFirstElement(f: (A ⇒ S)) ⇒ f(iter.next())
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
