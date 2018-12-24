package com.github.trembita.experimental.spark.streaming

import cats.Id
import cats.effect.IO
import com.github.trembita.experimental.spark.AsyncTimeout
import com.github.trembita.fsm.{FSM, InitialState}
import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.DStream

import scala.language.higherKinds
import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

@implicitNotFound(
  """
    Unable to run Finite state machine in context ${F} on Spark.
    Probably Spark doesn't support an efficient implementation with ${F} by default
  """
)
trait SparkStreamingFSM[F[_]] {
  def byKey[A: ClassTag, K: ClassTag, N, D, B: ClassTag](source: DStream[A])(
      getKey: A => K,
      initial: InitialState[N, D, F]
  )(fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]): DStream[B]
}

object SparkStreamingFSM {
  implicit val idFsm: SparkStreamingFSM[Id] = new SparkStreamingIDFsm
  implicit val ioFsm: SparkStreamingFSM[IO] = new SparkStreamingIOFsm
}

class SparkStreamingIDFsm extends SparkStreamingFSM[Id] {

  def byKey[A: ClassTag, K: ClassTag, N, D, B: ClassTag](source: DStream[A])(
      getKey: A => K,
      initial: InitialState[N, D, Id]
  )(fsmF: FSM.Empty[Id, N, D, A, B] => FSM.Func[Id, N, D, A, B]): DStream[B] = {

    val fsm = fsmF(new FSM.Empty[Id, N, D, A, B])

    val stateSpec: StateSpec[K, A, FSM.State[N, D, Id], Vector[B]] = StateSpec.function {
      (k: K, v: Option[A], state: State[FSM.State[N, D, Id]]) =>
        val currState: Option[FSM.State[N, D, Id]] = state.getOption match {
          case Some(state) => Some(state)
          case None =>
            initial match {
              case InitialState.Pure(state) => Some(state)
              case InitialState
                    .FromFirstElement(f: (A => FSM.State[N, D, Id])) =>
                v.map(f)
            }
        }

        val nextVs: Vector[B] = {
          val updated = for {
            state <- currState
            a     <- v
          } yield fsm(state)(a)
          updated.foreach(it => state.update(it._1))

          updated.map(_._2).toVector.flatten
        }
        nextVs
    }

    val mappedDStream: DStream[B] = source
      .map(a => getKey(a) -> a)
      .mapWithState(stateSpec)
      .flatMap(bs => bs)

    mappedDStream
  }
}

class SparkStreamingIOFsm extends SparkStreamingFSM[IO] {

  def byKey[A: ClassTag, K: ClassTag, N, D, B: ClassTag](source: DStream[A])(
      getKey: A => K,
      initial: InitialState[N, D, IO]
  )(fsmF: FSM.Empty[IO, N, D, A, B] => FSM.Func[IO, N, D, A, B]): DStream[B] = {

    val fsm = fsmF(new FSM.Empty[IO, N, D, A, B])

    val stateSpec: StateSpec[K, A, FSM.State[N, D, IO], Vector[B]] = StateSpec.function {
      (k: K, v: Option[A], state: State[FSM.State[N, D, IO]]) =>
        val currState: Option[FSM.State[N, D, IO]] = state.getOption match {
          case Some(state) => Some(state)
          case None =>
            initial match {
              case InitialState.Pure(state) => Some(state)
              case InitialState
                    .FromFirstElement(f: (A => FSM.State[N, D, IO])) =>
                v.map(f)
            }
        }

        val nextVs: Vector[B] = {
          val updated = for {
            state <- currState
            a     <- v
          } yield fsm(state)(a)
          updated.foreach(it => state.update(it.map(_._1).unsafeRunSync()))

          updated.map(_.map(_._2).unsafeRunSync()).toVector.flatten
        }
        nextVs
    }

    val mappedDStream: DStream[B] = source
      .map(a => getKey(a) -> a)
      .mapWithState(stateSpec)
      .flatMap(bs => bs)

    mappedDStream
  }
}
