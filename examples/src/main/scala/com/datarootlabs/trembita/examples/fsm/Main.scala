package com.datarootlabs.trembita.examples.fsm

import com.datarootlabs.trembita._
import com.datarootlabs.trembita.fsm._
import InitialState._
import FSM._
import scala.util.Random


object Main {
  sealed trait DoorState
  case object Opened extends DoorState
  case object Closed extends DoorState

  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[Int] = DataPipeline.from(5 to 10)
    val withDoorState = pipeline.mapWithState[DoorState, Int](initial = Pure(Opened), result = Result.withState)(_
      .when(Opened) {
        case i if i % 2 == 0 ⇒ (Closed, i + 1)
        case i               ⇒ (Opened, i * 2 + 30)
      }
      .when(Closed) {
        case i if i % 3 == 0 ⇒ (Opened, i * 2 + 100)
      }
      .whenUndefined { (_, i) ⇒ (Closed, i + 1050) }
    )

    val result: String = withDoorState.force.mkString("; ")
    println("Map with state:")
    println(result)
    println("--------------------------------------")

    val withState2 = pipeline.flatMapWithState[DoorState, Int](
      initial = FromFirstElement((i: Int) ⇒ if (i == 1) Closed else Opened),
      result = Result.ignoreState
    )(_
      .when(Opened) {
        case i if i % 2 == 0 ⇒ (Closed, 1 to (i + 1) map (_ + 100))
        case i               ⇒ (Opened, Random.shuffle(1 to i).map(i ⇒ i * i + 2000))
      }
      .when(Closed) {
        case i if i % 5 == 0 ⇒ (Opened, List(i - 50))
      }
      .whenUndefined { (s, i) ⇒ (s, List(i + 1997)) }
    )

    val result2: String = withState2.force.mkString(";\n")
    println("Flat map with state:")
    println(result2)
    println("--------------------------------------")
  }
}
