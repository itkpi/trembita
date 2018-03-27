package com.datarootlabs.trembita.fsm

sealed trait InitialState[S]
object InitialState {
  case class FromFirstElement[A, S](f: A ⇒ S) extends InitialState[S]
  case class Pure[S](state: S) extends InitialState[S]

  def fromFirstElement[A, S](f: A ⇒ S): InitialState[S] = FromFirstElement(f)
  def pure[S](state: S): InitialState[S] = Pure(state)
}

sealed trait FSM[S, A, B] {
  def when(state: S)(f: PartialFunction[A, (S, B)]): FSM.Partial[S, A, B]
  def complete: S ⇒ A ⇒ (S, B)
}

object FSM {
  sealed trait Result[A, S] {
    type Out
    def apply(state: S, value: A): Out
  }
  object Result {
    case class WithState[A, S]() extends Result[A, S] {
      type Out = (S, A)
      def apply(state: S, value: A): Out = state → value
    }
    case class IgnoreState[A, S]() extends Result[A, S] {
      type Out = A
      def apply(state: S, value: A): Out = value
    }

    def withState[A, S]: Result[A, S] = WithState()
    def ignoreState[A, S]: Result[A, S] = IgnoreState()
  }

  class Empty[S, A, B] extends FSM[S, A, B] {
    def when(state: S)(f: PartialFunction[A, (S, B)]): Partial[S, A, B] = new Partial[S, A, B]({
      case `state` ⇒ f
    })
    def complete: S ⇒ A ⇒ (S, B) = throw new Exception("Empty FSM")
  }

  class Completed[S, A, B](f: S ⇒ A ⇒ (S, B)) extends FSM[S, A, B] {
    def when(state: S)(f: PartialFunction[A, (S, B)]): Partial[S, A, B] = ???
    def complete: S ⇒ A ⇒ (S, B) = f
  }

  class Partial[S, A, B](stateF: Func[S, A, B]) extends FSM[S, A, B] {
    def when(state: S)(f: PartialFunction[A, (S, B)]): FSM.Partial[S, A, B] = {
      val pf: Func[S, A, B] = {case `state` ⇒ f}
      new Partial(stateF orElse pf)
    }

    def whenUndefined(f: (S, A) ⇒ (S, B)): FSM.Completed[S, A, B] = {
      val ff: S ⇒ A ⇒ (S, B) = state ⇒ a => f(state, a)
      new Completed({
        case state if stateF.isDefinedAt(state) ⇒ stateF(state).orElse({ case a ⇒ ff(state)(a) })
        case state                              ⇒ a => ff(state)(a)
      })
    }

    def complete: S ⇒ A ⇒ (S, B) = {
      case state if stateF.isDefinedAt(state) ⇒ a ⇒
        stateF(state).applyOrElse(a,
          throw new Exception(s"FSM in state {$state} is not defined at value: $a"))
      case state                              ⇒ _ => throw new Exception(s"FSM is not defined at state: $state")
    }
  }

  type Func[S, A, B] = PartialFunction[S, PartialFunction[A, (S, B)]]
  def apply[S, A, B]: FSM[S, A, B] = new Empty
}
