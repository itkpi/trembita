package com.datarootlabs.trembita.fsm

sealed trait InitialState[N, D]
object InitialState {
  case class FromFirstElement[A, N, D](f: A ⇒ FSM.State[N, D]) extends InitialState[N, D]
  case class Pure[N, D](state: FSM.State[N, D]) extends InitialState[N, D]

  def fromFirstElement[A, N, D](f: A ⇒ FSM.State[N, D]): InitialState[N, D] = FromFirstElement(f)
  def pure[N, D](state: FSM.State[N, D]): InitialState[N, D] = Pure(state)
}

sealed trait FSM[N, D, A, B] {
  def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[N, D, A, B]
  def complete: FSM.State[N, D] ⇒ A ⇒ (FSM.State[N, D], B)
}

object FSM {
  case class State[+N, +D](name: N, data: D) {
    def goto[NN >: N](other: NN): State[NN, D] = State(other, data)
    def stay: State[N, D] = this
    def change[DD >: D](otherData: DD): State[N, DD] = State(name, otherData)
    def modify[DD >: D](f: D ⇒ DD): State[N, DD] = State(name, f(data))

    def using[B](f: D ⇒ B): (State[N, D], B) = this → f(data)
  }

  sealed trait Result[A, S <: FSM.State[_, _]] {
    type Out
    def apply(state: S, value: A): Out
  }
  object Result {
    case class WithState[A, S <: FSM.State[_, _]]() extends Result[A, S] {
      type Out = (S, A)
      def apply(state: S, value: A): Out = state → value
    }
    case class IgnoreState[A, S <: FSM.State[_, _]]() extends Result[A, S] {
      type Out = A
      def apply(state: S, value: A): Out = value
    }

    def withState[A, S <: FSM.State[_, _]]: Result[A, S] = WithState()
    def ignoreState[A, S <: FSM.State[_, _]]: Result[A, S] = IgnoreState()
  }

  class Empty[N, D, A, B] extends FSM[N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[N, D, A, B] = new Partial[N, D, A, B]({
      case `state` ⇒ f
    })
    def complete: FSM.State[N, D] ⇒ A ⇒ (FSM.State[N, D], B) = throw new Exception("Empty FSM")
  }

  class Completed[N, D, A, B](f: FSM.State[N, D] ⇒ A ⇒ (FSM.State[N, D], B)) extends FSM[N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[N, D, A, B] = ???
    def complete: FSM.State[N, D] ⇒ A ⇒ (FSM.State[N, D], B) = f
  }

  class Partial[N, D, A, B](stateF: Func[N, D, A, B]) extends FSM[N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[N, D, A, B] = {
      val pf: Func[N, D, A, B] = {case `state` ⇒ f}
      new Partial(stateF orElse pf)
    }

    def whenUndefined(f: A ⇒ FSM.State[N, D] ⇒ (FSM.State[N, D], B)): FSM.Completed[N, D, A, B] = {
      new Completed({
        case state if stateF.isDefinedAt(state.name) ⇒ a ⇒
          val nameF = stateF(state.name)
          if (nameF.isDefinedAt(a)) nameF(a)(state)
          else f(a)(state)

        case state ⇒ a => f(a)(state)
      })
    }

    def complete: FSM.State[N, D] ⇒ A ⇒ (FSM.State[N, D], B) = {
      case state if stateF.isDefinedAt(state.name) ⇒ a ⇒
        stateF(state.name).applyOrElse(a,
          throw new Exception(s"FSM in state {$state} is not defined at value: $a"))(state)
      case state                                   ⇒ _ => throw new Exception(s"FSM is not defined at state: $state")
    }
  }

  type Func[N, D, A, B] = PartialFunction[N, PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]]
  def apply[N, D, A, B]: FSM[N, D, A, B] = new Empty
}
