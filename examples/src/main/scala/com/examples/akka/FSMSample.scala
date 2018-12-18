package com.examples.akka

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl._
import cats.effect.{ExitCode, IO, IOApp}
import com.github.trembita.DataPipelineT
import cats.effect.Console.io._
import com.github.trembita._
import com.github.trembita.fsm._
import com.github.trembita.collections._
import com.github.trembita.experimental.akka._
import cats.implicits._

import scala.concurrent.ExecutionContext
import scala.io.StdIn
import scala.util.Random

object FSMSample extends IOApp {
  sealed trait DoorState
  case object Opened extends DoorState
  case object Closed extends DoorState

  def akkaTrembitaFsmSample(implicit mat: ActorMaterializer,
                            ec: ExecutionContext): IO[Unit] = {
    val pipeline: DataPipelineT[IO, Int, Akka] =
      DataPipelineT.fromRepr[IO, Int, Akka](
        Source.fromIterator(() => Iterator.continually(Random.nextInt()))
      )

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

    val killSwitchIO: IO[UniqueKillSwitch] =
      withDoorState.evalRepr
        .flatMap(
          source =>
            IO {
              source
                .viaMat(KillSwitches.single)(Keep.right)
                .to(Sink.foreach(println))
                .run()
          }
        )

    for {
      killSwitch <- killSwitchIO
      _ <- IO { StdIn.readLine("Press something to stop") }
      _ <- IO { killSwitch.shutdown() }
      _ <- putStrLn("Stopped!")
    } yield {}
  }

  def run(args: List[String]): IO[ExitCode] =
    IO {
      ActorSystem("trembita-akka")
    }.bracket(use = { implicit system: ActorSystem =>
        akkaTrembitaFsmSample(ActorMaterializer(), system.dispatcher)
      })(
        release = system =>
          IO.fromFuture(IO {
              system.terminate()
            })
            .void
      )
      .as(ExitCode.Success)
}
