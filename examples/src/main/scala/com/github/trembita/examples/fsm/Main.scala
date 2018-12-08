package com.github.trembita.examples.fsm

import com.github.trembita._
import com.github.trembita.fsm._
import com.github.trembita.examples.putStrLn
import com.github.trembita.collections._
import InitialState._
import Execution._
import FSM._
import cats.effect._
import cats.implicits._

object Main extends IOApp {
  sealed trait DoorState
  case object Opened extends DoorState
  case object Closed extends DoorState

  def run(args: List[String]): IO[ExitCode] = {
    val pipeline: DataPipelineT[IO, Int, Sequential] =
      DataPipelineT.randomInts(20, 100)

    val withDoorState = pipeline.fsm[DoorState, Map[DoorState, Int], Int](
      initial = Pure(FSM.State(Opened, Map.empty))
    )(_.when(Opened) {
      case i if i % 2 == 0 =>
        _.goto(Closed)
          .modify(_.modify(Opened, default = 1)(_ + 1))
          .push(_(Opened) + i)
      case i if i % 4 == 0 => _.stay push (i * 2)
    }.when(Closed) {
        case i if i % 3 == 0 =>
          _.goto(Opened)
            .modify(_.modify(Closed, default = 1)(_ + 1)) spam (_(Closed) to 10)
        case i if i % 2 == 0 =>
          _.stay.pushF { data =>
            IO { data.values.sum }
          }
      }
      .whenUndefined { i =>
        {
          println(s"Producing nothing..! [#$i]")
          _.goto(Closed).change(Map.empty).dontPush
        }
      })

    val result: IO[Vector[Int]] = withDoorState.eval
    result
      .flatTap { result =>
        putStrLn("Map with state:") *>
          putStrLn(result) *>
          putStrLn("--------------------------------------")
      }
      .as(ExitCode.Success)
  }
}
