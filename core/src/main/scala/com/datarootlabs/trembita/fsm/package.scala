package com.datarootlabs.trembita

import com.datarootlabs.trembita.DataPipeline
import com.datarootlabs.trembita.internal.StrictSource


package object fsm {
  implicit class StatefulOps[A](val self: DataPipeline[A]) extends AnyVal {
    def mapWithState[S, B](initialState: S)(stateF: S ⇒ A ⇒ (S, B)): DataPipeline[(S, B)] = new StrictSource[(S, B)]({
      val iter = self.iterator
      val builder = Vector.newBuilder[(S, B)]
      var state: S = initialState
      while (iter.hasNext) {
        val elem = iter.next()
        val (newState, b) = stateF(state)(elem)
        state = newState
        builder += state → b
      }
      builder.result()
    })

    def statefulMap[S, B](initialState: S)(stateF: S ⇒ A ⇒ (S, B)): DataPipeline[B] =
      mapWithState(initialState)(stateF).map(_._2)

    def flatMapWithState[S, B](initialState: S)(stateF: S ⇒ A ⇒ (S, Iterable[B])): DataPipeline[(S, B)] = {
      val iter = self.iterator
      val builder = Vector.newBuilder[Iterable[(S, B)]]
      var state: S = initialState
      while (iter.hasNext) {
        val elem = iter.next()
        val (newState, b) = stateF(state)(elem)
        state = newState
        builder += b.map(state → _)
      }
      builder.result().reduce(_ ++ _)
    }

    def statefulFlatMap[S, B](initialState: S)(stateF: S ⇒ A ⇒ (S, Iterable[B])): DataPipeline[B] =
      flatMapWithState(initialState)(stateF).map(_._2)
  }
}
