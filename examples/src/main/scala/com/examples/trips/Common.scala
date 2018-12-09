package com.examples.trips

import cats.effect.Sync
import cats.implicits._
import com.github.trembita._
import com.github.trembita.fsm._
import scala.language.higherKinds

object Aliases {
  trait `unit id`
  trait `driver id`
  trait `trip type`
  trait `covered distance`
  trait `trip duration`
  trait `idle duration`
  trait `max covered distance`
  trait `max idle time`
  trait `time in trip`
}

object Common {
  sealed trait UnitState
  case object Driving extends UnitState
  case object Idle extends UnitState
  case class FirstMsg(firstMsg: UnitMessage)

  def getActivities[F[_], Ex <: Execution](
    messagesPipeline: DataPipelineT[F, UnitMessage, Ex]
  )(implicit F: Sync[F]): DataPipelineT[F, DrivingActivity, Ex] = {
    messagesPipeline.fsm[UnitState, FirstMsg, DrivingActivity](
      InitialState.fromFirstElement(
        (msg: UnitMessage) =>
          FSM.State(if (msg.ignitionOn) Driving else Idle, FirstMsg(msg))
      )
    )(_.when(Driving) {
      case msg if msg.ignitionOff =>
        _.goto(Idle).modPush {
          case FirstMsg(firstMsg) if msg.unitId == firstMsg.unitId =>
            FirstMsg(msg) ->
              DrivingActivity(
                firstMsg.unitId,
                firstMsg.driverId,
                startDate = firstMsg.timestamp,
                endDate = msg.timestamp,
                ignitionOn = true,
                startLocation = LocationInfo(
                  firstMsg.latitude,
                  firstMsg.longitude,
                  firstMsg.address
                ),
                endLocation =
                  LocationInfo(msg.latitude, msg.longitude, msg.address),
                startOdometer = firstMsg.odometerKms,
                endOdometer = msg.odometerKms,
                tripType = firstMsg.tripType,
                startFuelLevel = firstMsg.fuelLevel,
                endFuelLevel = msg.fuelLevel
              ).some
          case _ => FirstMsg(msg) -> None
        }
    }.when(Idle) {
        case msg if msg.ignitionOn =>
          _.goto(Driving).modPush {
            case FirstMsg(firstMsg) if msg.unitId == firstMsg.unitId =>
              val location = LocationInfo(
                firstMsg.latitude,
                firstMsg.longitude,
                firstMsg.address
              )
              FirstMsg(msg) ->
                DrivingActivity(
                  firstMsg.unitId,
                  firstMsg.driverId,
                  startDate = firstMsg.timestamp,
                  endDate = msg.timestamp,
                  ignitionOn = false,
                  startLocation = location,
                  endLocation = location,
                  startOdometer = 0.0,
                  endOdometer = 0.0,
                  tripType = TripType.Unknown,
                  startFuelLevel = None,
                  endFuelLevel = None
                ).some
            case _ => FirstMsg(msg) -> None
          }
      }
      .whenUndefined(_ => _.stay.dontPush))
  }
}
