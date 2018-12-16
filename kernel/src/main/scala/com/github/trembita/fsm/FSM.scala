package com.github.trembita.fsm

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
sealed trait InitialState[N, D, F[_]]
object InitialState {

  /**
    * [[InitialState]] from first element of the pipeline
    *
    * @tparam A - pipeline data type
    * @tparam N - named state
    * @tparam D - state data
    * @param f - function to extract the initial state
    **/
  case class FromFirstElement[A, N, D, F[_]](f: A => FSM.State[N, D, F])
      extends InitialState[N, D, F]

  /**
    * Pure [[InitialState]]
    *
    * @tparam N - named state
    * @tparam D - state data
    * @param state - the initial state itself
    **/
  case class Pure[N, D, F[_]](state: FSM.State[N, D, F])
      extends InitialState[N, D, F]

  /**
    * Get the initial state for first element
    *
    * @tparam A - pipeline data type
    * @tparam N - named state
    * @tparam D - state data
    * @param f - function to extract the initial state
    * @return - initial state
    **/
  def fromFirstElement[A, N, D, F[_]](
    f: A => FSM.State[N, D, F]
  ): InitialState[N, D, F] = FromFirstElement(f)

  /**
    * Get pure initial state
    *
    * @tparam N - named state
    * @tparam D - state data
    * @param state - the initial state itself
    * @return - initial state of the FSM
    **/
  def pure[N, D, F[_]](state: FSM.State[N, D, F]): InitialState[N, D, F] =
    Pure(state)
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
sealed trait FSM[F[_], N, D, A, B] extends Serializable {

  /**
    * Extends behavior of the [[FSM]]
    *
    * @param state - some named state
    * @param f     - function that may change [[FSM]] behavior
    * @return - partially completed FSM
    **/
  def when(state: N)(f: PartialFunction[A, FSM.State[N, D, F] => F[
                       (FSM.State[N, D, F], Iterable[B])
                     ]]): FSM.Partial[F, N, D, A, B]
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
  case class State[N, D, F[_]](name: N, data: D) {

    /**
      * Goto other state
      *
      * @param other - other state name
      * @return - other state with the same data
      **/
    def goto(other: N): State[N, D, F] = State(other, data)

    /**
      * Stay in the same state
      *
      * @return - this
      **/
    def stay: State[N, D, F] = this

    /**
      * Change state's data
      *
      * @param otherData - new data for the state
      * @return - the same state with other data
      **/
    def change(otherData: D): State[N, D, F] = State(name, otherData)

    /**
      * Modify state's data using some function
      *
      * @param f - transformation function
      * @return - the same state with modified data
      **/
    def modify(f: D => D): State[N, D, F] = State(name, f(data))

    /**
      * Produce an output value
      * using state's data
      *
      * @param f - produce a value from state's data
      * @return - the same state with output value
      **/
    def push[B](
      f: D => B
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.pure(this → List(f(data)))

    def modPush[B](
      f: D => (D, Option[B])
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] = {
      val (newData, result) = f(data)
      (State[N, D, F](name, newData), result.toList: Iterable[B]).pure[F]
    }

    def pushF[B](
      f: D => F[B]
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.map(f(data))(b => this → List(b))

    /**
      * Produces the same value
      * ignoring state data
      *
      * @param value - value to produce
      * @return - the same state with output value
      **/
    def push[B](
      value: B
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.pure(this → List(value))

    def pushF[B](
      valueF: F[B]
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.map(valueF)(v => this → List(v))

    def dontPush[B](
      implicit F: Applicative[F]
    ): F[(State[N, D, F], Iterable[B])] = F.pure(this → Nil)

    def spam[B](
      f: D => Iterable[B]
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.pure(this → f(data))

    def spamF[B](
      f: D => F[Iterable[B]]
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.map(f(data))(this → _)

    def spam[B](
      values: Iterable[B]
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.pure(this → values)

    def spamF[B](
      valuesF: F[Iterable[B]]
    )(implicit F: Applicative[F]): F[(State[N, D, F], Iterable[B])] =
      F.map(valuesF)(this → _)
  }

  /**
    * A sum type representing FSM output result
    *
    * @tparam A - input data type
    * @tparam S - some [[FSM.State]]
    **/
  sealed trait Result[A, S] {

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
    case class WithState[A, S]() extends Result[A, S] {
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
    case class IgnoreState[A, S]() extends Result[A, S] {
      type Out = A
      def apply(state: S, value: A): Out = value
    }

    def withState[A, S]: Result[A, S] = WithState()
    def ignoreState[A, S]: Result[A, S] = IgnoreState()
  }

  /**
    * FSM without behavior
    *
    * @tparam N - named state
    * @tparam D - state data type
    * @tparam A - input type
    * @tparam B - resulting type
    **/
  class Empty[F[_]: Applicative, N, D, A, B] extends FSM[F, N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D, F] => F[
                         (FSM.State[N, D, F], Iterable[B])
                       ]]): FSM.Partial[F, N, D, A, B] =
      new Partial[F, N, D, A, B]({
        case `state` => {
          case a if f.isDefinedAt(a) =>
            s =>
              f(a)(s)
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
  class Partial[F[_]: Applicative, N, D, A, B](
    stateF: PartialFunc[F, N, D, A, B]
  ) extends FSM[F, N, D, A, B] {
    def when(state: N)(f: PartialFunction[A, FSM.State[N, D, F] => F[
                         (FSM.State[N, D, F], Iterable[B])
                       ]]): FSM.Partial[F, N, D, A, B] = {
      val pf: PartialFunc[F, N, D, A, B] = {
        case `state` => {
          case a if f.isDefinedAt(a) =>
            d =>
              f(a)(d)
        }
      }
      new Partial(stateF orElse pf)
    }

    def whenUndefined(
      f: A => FSM.State[N, D, F] => F[(FSM.State[N, D, F], Iterable[B])]
    ): FSM.State[N, D, F] => A => F[(FSM.State[N, D, F], Iterable[B])] = {
      case state if stateF.isDefinedAt(state.name) =>
        a =>
          val nameF = stateF(state.name)
          if (nameF.isDefinedAt(a)) nameF(a)(state)
          else f(a)(state)

      case state =>
        a =>
          f(a)(state)
    }
  }

  type PartialFunc[F[_], N, D, A, B] =
    PartialFunction[N, PartialFunction[A, FSM.State[N, D, F] => F[
      (FSM.State[N, D, F], Iterable[B])
    ]]]
  type Func[F[_], N, D, A, B] =
    FSM.State[N, D, F] => A => F[(FSM.State[N, D, F], Iterable[B])]

  /**
    * @tparam N - named state
    * @tparam D - state data type
    * @tparam A - input type
    * @tparam B - resulting type
    * @return - an empty [[FSM]]
    **/
  def apply[F[_]: Applicative, N, D, A, B]: FSM[F, N, D, A, B] = new Empty
}
