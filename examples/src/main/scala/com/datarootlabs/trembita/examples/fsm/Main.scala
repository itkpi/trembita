package com.datarootlabs.trembita.examples.fsm

import com.datarootlabs.trembita._
import com.datarootlabs.trembita.fsm._
import com.datarootlabs.trembita.utils._
import InitialState._
import FSM._
import scala.util.Random


object Main {
  sealed trait DoorState
  case object Opened extends DoorState
  case object Closed extends DoorState

  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[Int] = DataPipeline.from(5 to 10)
    val withDoorState = pipeline.statefulMap[DoorState, Map[DoorState, Int], Int](
      initial = Pure(FSM.State(Opened, Map.empty)),
      result = Result.withState
    )(_
      .when(Opened) {
        case i if i % 2 == 0 ⇒ _.goto(Closed).modify(_.modify(Opened, default = 1)(_ + 1)).produce(_ (Opened) + i)
        case i               ⇒ _.stay produce (i * 2)
      }
      .when(Closed) {
        case i if i % 3 == 0 ⇒ _.goto(Opened).modify(_.modify(Closed, default = 1)(_ + 1)) produce (i * 2)
      }
      .whenUndefined { i ⇒ _.goto(Closed).change(Map.empty) produce (i * 2) }
    )

    val result: String = withDoorState.eval.mkString(" ~>\n")
    println("Map with state:")
    println(result)
    println("--------------------------------------")
  }
}
