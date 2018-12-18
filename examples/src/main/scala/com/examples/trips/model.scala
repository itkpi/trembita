package com.examples.trips

import java.time._
import cats.Show
import scala.concurrent.duration.FiniteDuration

object TripType extends Enumeration {
  val Private, Business, Unknown = Value
}

case class UnitMessage(unitId: String,
                       driverId: Option[String],
                       timestamp: LocalDateTime,
                       latitude: Double,
                       longitude: Double,
                       address: Option[String],
                       ignitionOn: Boolean,
                       speed: Double,
                       odometerKms: Double,
                       tripType: TripType.Value,
                       fuelLevel: Option[Double]) {
  def ignitionOff: Boolean = !ignitionOn
}

case class LocationInfo(latitude: Double,
                        longitude: Double,
                        address: Option[String])

case class DrivingActivity(unitId: String,
                           driverId: Option[String],
                           startDate: LocalDateTime,
                           endDate: LocalDateTime,
                           ignitionOn: Boolean,
                           startLocation: LocationInfo,
                           endLocation: LocationInfo,
                           startOdometer: Double,
                           endOdometer: Double,
                           tripType: TripType.Value,
                           startFuelLevel: Option[Double],
                           endFuelLevel: Option[Double]) {
  def coveredDistance: Double = endOdometer - startOdometer
  def activityDuration: FiniteDuration = startDate until endDate
  def fuelConsumption: Option[Double] =
    for {
      endLevel <- endFuelLevel
      startLevel <- startFuelLevel
    } yield endLevel - startLevel
}
object DrivingActivity {

  implicit object ShowActivity extends Show[DrivingActivity] {
    def show(t: DrivingActivity): String = {
      s"""
         |Main info: unit: ${t.unitId}| driver: ${t.driverId}|
         |${if (t.ignitionOn) "Trip" else "Idle"} info: trip_type: ${t.tripType} from: ${t.startDate}| to: ${t.endDate}, start_location: ${t.startLocation}, end_location: ${t.endLocation}
         |Metrics: covered distance: ${t.coveredDistance}, duration: ${t.activityDuration}, fuel_consumption: ${t.fuelConsumption}
       """.stripMargin
    }
  }
}
