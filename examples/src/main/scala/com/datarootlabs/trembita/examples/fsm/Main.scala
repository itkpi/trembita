package com.datarootlabs.trembita.examples.fsm

import com.datarootlabs.trembita._
import com.datarootlabs.trembita.fsm._
import com.datarootlabs.trembita.collections._
import scala.util.Try
import Finiteness._
import InitialState._
import Execution._
import FSM._
import scala.util.Random
import cats.implicits._
import scala.concurrent.{Future, Await, ExecutionContext}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._


object Main {
  sealed trait DoorState
  case object Opened extends DoorState
  case object Closed extends DoorState

  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[Int, Future, Infinite, Sequential] = DataPipeline.infinite(Random.nextInt(100))

    val withDoorState = pipeline.fsm[DoorState, Map[DoorState, Int], Int](initial = Pure(FSM.State(Opened, Map.empty)))(_
      .when(Opened) {
        case i if i % 2 == 0 ⇒ _.goto(Closed).modify(_.modify(Opened, default = 1)(_ + 1)).push(_ (Opened) + i)
        case i if i % 4 == 0 ⇒ _.stay push (i * 2)
      }
      .when(Closed) {
        case i if i % 3 == 0 ⇒ _.goto(Opened).modify(_.modify(Closed, default = 1)(_ + 1)) spam (_ (Closed) to 10)
        case i if i % 2 == 0 ⇒ _.stay.pushF { data ⇒ Future {data.values.sum} }
      }
      .whenUndefined { i ⇒ {
        println(s"Producing nothing..! [#$i]")
        _.goto(Closed).change(Map.empty).dontPush
      }
      }
    )

    val result: Future[Unit] = withDoorState.consume { res ⇒
      println(s"~>$res")
      Future.unit
    }
    println("Map with state:")
    println(result)
    println("Oops, forgotten Await...")
    Thread.sleep(2000)
    println(Await.result(result, Duration.Inf))
    println("--------------------------------------")
  }
}
