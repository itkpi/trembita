package com.datarootlabs.trembita.fsm


import scala.language.higherKinds
import cats._
import cats.implicits._


/**
  * A sum type
  * representing initial state for [[FSM]]
  *
  * @tparam N - named state
  * @tparam D - state data
  **/
sealed trait InitialState[N, D]
object InitialState {
  /**
    * [[InitialState]] from first element of the pipeline
    *
    * @tparam A - pipeline data type
    * @tparam N - named state
    * @tparam D - state data
    * @param f - function to extract the initial state
    **/
  case class FromFirstElement[A, N, D](f: A ⇒ FSM.State[N, D]) extends InitialState[N, D]

  /**
    * Pure [[InitialState]]
    *
    * @tparam N - named state
    * @tparam D - state data
    * @param state - the initial state itself
    **/
  case class Pure[N, D](state: FSM.State[N, D]) extends InitialState[N, D]

  /**
    * Get the initial state for first element
    *
    * @tparam A - pipeline data type
    * @tparam N - named state
    * @tparam D - state data
    * @param f - function to extract the initial state
    * @return - initial state
    **/
  def fromFirstElement[A, N, D](f: A ⇒ FSM.State[N, D]): InitialState[N, D] = FromFirstElement(f)

  /**
    * Get pure initial state
    *
    * @tparam N - named state
    * @tparam D - state data
    * @param state - the initial state itself
    * @return - initial state of the FSM
    **/
  def pure[N, D](state: FSM.State[N, D]): InitialState[N, D] = Pure(state)
}

/**
  * A sum type
  * representing some Finite State Machine builder
  *
  * @tparam F - monad in wich the result will be wrapped in
  * @tparam N - named state
  * @tparam D - state data type
  * @tparam A - input type
  * @tparam B - resulting type
  **/
sealed trait FSM[F[_], N, D, A, B] {
  /**
    * Extends behavior of the [[FSM]]
    *
    * @param state - some named state
    * @param f     - function that may change [[FSM]] behavior
    * @return - partially completed FSM
    **/
  def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[F, N, D, A, B]
  def whenF(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ F[(FSM.State[N, D], B)]]): FSM.Partial[F, N, D, A, B]
}

object FSM {
  /**
    * A product of some named state[[N]]
    * and its data [[D]]
    *
    * @tparam N - named state
    * @tparam D - state data type
    * @param name - state name
    * @param data - state's data
    **/
  case class State[+N, +D](name: N, data: D) {
    /**
      * Goto other state
      *
      * @param other - other state name
      * @return - other state with the same data
      **/
    def goto[NN >: N](other: NN): State[NN, D] = State(other, data)

    /**
      * Stay in the same state
      *
      * @return - this
      **/
    def stay: State[N, D] = this

    /**
      * Change state's data
      *
      * @param otherData - new data for the state
      * @return - the same state with other data
      **/
    def change[DD >: D](otherData: DD): State[N, DD] = State(name, otherData)

    /**
      * Modify state's data using some function
      *
      * @param f - transformation function
      * @return - the same state with modified data
      **/
    def modify[DD >: D](f: D ⇒ DD): State[N, DD] = State(name, f(data))

    /**
      * Produce an output value
      * using state's data
      *
      * @param f - produce a value from state's data
      * @return - the same state with output value
      **/
    def produce[B](f: D ⇒ B): (State[N, D], B) = this → f(data)

    /**
      * Produces the same value
      * ignoring state data
      *
      * @param value - value to produce
      * @return - the same state with output value
      **/
    def produce[B](value: B): (State[N, D], B) = this → value
  }

  /**
    * A sum type representing FSM output result
    *
    * @tparam A - input data type
    * @tparam S - some [[FSM.State]]
    **/
  sealed trait Result[A, S <: FSM.State[_, _]] {
    /** Output type of the FSM */
    type Out

    /**
      * Extract [[Out]] from State [[S]] and the value [[A]]
      *
      * @param state - the state
      * @param value - the value
      * @return - output
      **/
    def apply(state: S, value: A): Out
  }

  object Result {
    /**
      * [[FSM.Result]] implementation
      * producing a pair of State [[S]] and value [[A]]
      *
      * @tparam A - input data type
      * @tparam S - some [[FSM.State]]
      **/
    protected[trembita]
    case class WithState[A, S <: FSM.State[_, _]]() extends Result[A, S] {
      type Out = (S, A)
      def apply(state: S, value: A): Out = state → value
    }

    /**
      * [[FSM.Result]] implementation
      * producing a value [[A]] ignoring State [[S]]
      *
      * @tparam A - input data type
      * @tparam S - some [[FSM.State]]
      **/
    protected[trembita]
    case class IgnoreState[A, S <: FSM.State[_, _]]() extends Result[A, S] {
      type Out = A
      def apply(state: S, value: A): Out = value
    }


    def withState[A, S <: FSM.State[_, _]]: Result[A, S] = WithState()
    def ignoreState[A, S <: FSM.State[_, _]]: Result[A, S] = IgnoreState()
  }

  /**
    * FSM without behavior
    *
    * @tparam N - named state
    * @tparam D - state data type
    * @tparam A - input type
    * @tparam B - resulting type
    **/
  protected[trembita]
  class Empty[F[_] : Applicative, N, D, A, B] extends FSM[F, N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[F, N, D, A, B] =
      new Partial[F, N, D, A, B]({
        case `state` ⇒ {
          case a if f.isDefinedAt(a) ⇒ s ⇒ f(a)(s).pure[F]
        }
      })

    def whenF(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ F[(FSM.State[N, D], B)]]): FSM.Partial[F, N, D, A, B] =
      new Partial[F, N, D, A, B]({
        case `state` ⇒ {
          case a if f.isDefinedAt(a) ⇒ s ⇒ f(a)(s)
        }
      })
  }

  /**
    * FSM with some behavior defined
    * (BUT NOT COMPLETED YET)
    *
    * @tparam N - named state
    * @tparam D - state data type
    * @tparam A - input type
    * @tparam B - resulting type
    * @param stateF - partial function handling some behavior
    **/
  protected[trembita]
  class Partial[F[_] : Applicative, N, D, A, B](stateF: PartialFunc[F, N, D, A, B]) extends FSM[F, N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ (FSM.State[N, D], B)]): FSM.Partial[F, N, D, A, B] = {
      val pf: PartialFunc[F, N, D, A, B] = {
        case `state` ⇒ {
          case a if f.isDefinedAt(a) ⇒ d ⇒ f(a)(d).pure[F]
        }
      }
      new Partial(stateF orElse pf)
    }

    def whenF(state: N)(f: PartialFunction[A, FSM.State[N, D] ⇒ F[(FSM.State[N, D], B)]]): FSM.Partial[F, N, D, A, B] = {
      val pf: PartialFunc[F, N, D, A, B] = {
        case `state` ⇒ {
          case a if f.isDefinedAt(a) ⇒ d ⇒ f(a)(d)
        }
      }
      new Partial(stateF orElse pf)
    }

    def whenUndefined(f: A ⇒ FSM.State[N, D] ⇒ (FSM.State[N, D], B)): FSM.State[N, D] ⇒ A ⇒ F[(FSM.State[N, D], B)] = {
      case state if stateF.isDefinedAt(state.name) ⇒ a ⇒
        val nameF = stateF(state.name)
        if (nameF.isDefinedAt(a)) nameF(a)(state)
        else f(a)(state).pure[F]

      case state ⇒ a ⇒ f(a)(state).pure[F]
    }

    def whenUndefinedF(f: A ⇒ FSM.State[N, D] ⇒ F[(FSM.State[N, D], B)]): FSM.State[N, D] ⇒ A ⇒ F[(FSM.State[N, D], B)] = {
      case state if stateF.isDefinedAt(state.name) ⇒ a ⇒
        val nameF = stateF(state.name)
        if (nameF.isDefinedAt(a)) nameF(a)(state)
        else f(a)(state)

      case state ⇒ a ⇒ f(a)(state)
    }
  }


  type PartialFunc[F[_], N, D, A, B] = PartialFunction[N, PartialFunction[A, FSM.State[N, D] ⇒ F[(FSM.State[N, D], B)]]]
  type Func[F[_], N, D, A, B] = FSM.State[N, D] ⇒ A ⇒ F[(FSM.State[N, D], B)]
  /**
    * @tparam N - named state
    * @tparam D - state data type
    * @tparam A - input type
    * @tparam B - resulting type
    * @return - an empty [[FSM]]
    **/
  def apply[F[_] : Applicative, N, D, A, B]: FSM[F, N, D, A, B] = new Empty
}
