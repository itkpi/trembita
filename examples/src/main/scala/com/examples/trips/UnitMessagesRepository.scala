package com.examples.trips

import java.time.{LocalDateTime => JLocalDateTime}
import com.outworkers.phantom.connectors.CassandraConnection
import com.outworkers.phantom.dsl._
import com.outworkers.phantom.jdk8._

abstract class UnitMessages extends Table[UnitMessages, UnitMessage] {
  object unitId extends StringColumn with PrimaryKey {
    override val name = "unit_id"
  }
  object timestamp extends Col[JLocalDateTime] with PartitionKey
  object driverId extends OptionalStringColumn {
    override val name = "driver_id"
  }
  object latitude    extends DoubleColumn
  object longitude   extends DoubleColumn
  object address     extends OptionalStringColumn
  object ignitionOn  extends BooleanColumn { override val name = "ignition_on" }
  object speed       extends DoubleColumn
  object odometerKms extends DoubleColumn { override val name = "odometer_kms" }
  object tripType extends EnumColumn[TripType.Value] {
    override val name = "trip_type"
  }
  object fuelLevel extends OptionalDoubleColumn {
    override val name = "fuel_level"
  }
}

class UnitMessagesRepository(connector: CassandraConnection) extends Database[UnitMessagesRepository](connector) {

  object unitMessages extends UnitMessages with Connector

  private def insertStatement(e: UnitMessage) =
    unitMessages.insert
      .value(_.unitId, e.unitId)
      .value(_.timestamp, e.timestamp)
      .value(_.driverId, e.driverId)
      .value(_.latitude, e.latitude)
      .value(_.longitude, e.longitude)
      .value(_.address, e.address)
      .value(_.ignitionOn, e.ignitionOn)
      .value(_.speed, e.speed)
      .value(_.odometerKms, e.odometerKms)
      .value(_.tripType, e.tripType)
      .value(_.fuelLevel, e.fuelLevel)

  def selectByUnits(unitIds: List[String], fromDate: JLocalDateTime, toDate: JLocalDateTime) =
    unitMessages.select
      .where(_.unitId in unitIds)
      .and(_.timestamp gte fromDate)
      .and(_.timestamp lte toDate)

  def selectByUnit(unitId: String, fromDate: JLocalDateTime, toDate: JLocalDateTime) =
    unitMessages.select
      .where(_.unitId eqs unitId)
      .and(_.timestamp gte fromDate)
      .and(_.timestamp lte toDate)

  def selectAllStatement = unitMessages.select
}
