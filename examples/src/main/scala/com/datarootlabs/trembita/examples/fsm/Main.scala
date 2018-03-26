package com.datarootlabs.trembita.examples.fsm

import com.datarootlabs.trembita._
import com.datarootlabs.trembita.fsm._
import scala.util.Random


sealed trait DoorState
case object Opened extends DoorState
case object Closed extends DoorState

object Main {
  def main(args: Array[String]): Unit = {
    val pipeline: DataPipeline[Int] = DataPipeline.from(1 to 5)
    val withDoorState: DataPipeline[(DoorState, Int)] = pipeline.mapWithState(Opened: DoorState)(
      FSM[DoorState, Int, Int]
        .when(Opened) {
          case i if i % 2 == 0 ⇒ (Closed, i + 1)
          case i               ⇒ (Opened, i * 2 + 30)
        }
        .when(Closed) {
          case i if i % 3 == 0 ⇒ (Opened, i * 2 + 100)
        }
        .whenUndefined { (_, i) ⇒ (Closed, i + 1050) }
        .complete
    )
    val result: String = withDoorState.force.mkString("; ")
    println("Map with state:")
    println(result)
    println("--------------------------------------")

    val withState2: DataPipeline[(DoorState, Int)] = pipeline.flatMapWithState(Opened: DoorState) {
      FSM[DoorState, Int, Iterable[Int]]
        .when(Opened) {
          case i if i % 2 == 0 ⇒ (Closed, 1 to (i + 1) map (_ + 100))
          case i               ⇒ (Opened, Random.shuffle(1 to i).map(i ⇒ i * i + 2000))
        }
        .when(Closed) {
          case i if i % 5 == 0 ⇒ (Opened, List(i - 50))
        }
        .whenUndefined { (s, i) ⇒ (s, List(i + 1997)) }
        .complete
    }
    val result2: String = withState2.force.mkString(";\n")
    println("Flat map with state:")
    println(result2)
    println("--------------------------------------")
  }
}
