package com.examples.kernel

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.Console.io._
import trembita._
import trembita.fsm._
import trembita.collections._
import cats.implicits._

object FSMSample extends IOApp {
  sealed trait DoorState
  case object Opened extends DoorState
  case object Closed extends DoorState

  def run(args: List[String]): IO[ExitCode] = {
    val pipeline: DataPipelineT[IO, Int, Sequential] =
      Input.randomF[IO].create(RandomInput.propsT[IO, Int](n = 20, count = 100)(x => IO { x + 2 }))

    val withDoorState =
      pipeline.fsm[DoorState, Map[DoorState, Int], Int](
        initial = InitialState.pure(FSM.State(Opened, Map.empty))
      )(_.when(Opened) {
        case i if i % 2 == 0 =>
          _.goto(Closed)
            .modify(_.modify(Opened, default = 1)(_ + 1))
            .push(_.apply(Opened) + i)
        case i if i % 4 == 0 => _.stay push (i * 2)
      }.when(Closed) {
          case i if i % 3 == 0 =>
            _.goto(Opened)
              .modify(_.modify(Closed, default = 1)(_ + 1)) spam (_.apply(
              Closed
            ) to 10)
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

    val result: IO[Vector[Int]] = withDoorState.into(Output.collection[Vector]).run
    result
      .flatTap { result =>
        putStrLn("Map with state:") *>
          putStrLn(result) *>
          putStrLn("--------------------------------------")
      }
      .as(ExitCode.Success)
  }
}
