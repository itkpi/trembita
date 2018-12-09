# Trembita kernel

TBD;

## Basic
For basic operations look at [basic sample](../examples/src/main/scala/com/examples/kernel/Basic.scala)
For joins look at [join sample](../examples/src/main/scala/com/examples/kernel/JoinsSample.scala)

## QL
Look at [QL sample](../examples/src/main/scala/com/examples/kernel/QLSample.scala).

## FSM
Loot at [FSM sample](../examples/src/main/scala/com/examples/kernel/FSMSample.scala)

## Serialization
Transforms aggregated data directly at [Circe sample](../examples/src/main/scala/com/examples/trembitacirce/Main.scala)

## Complex sample
[Trips](../examples/src/main/scala/com/examples/trips) contains a real-life example of processing real-time vehicle data.

Having a [message](../examples/src/main/scala/com/examples/trips/model.scala) with different metrics
using FSM you are allowed to define generic [transformation](../examples/src/main/scala/com/examples/trips/Common.scala) from single message into `DrivingActivity` (trip either idle).
Based on those activities you are allowed to aggregate your data using QL into some kind of [report](../examples/src/main/scala/com/examples/trips/SampleReport.scala).

Data is fetched from Cassandra using [phantom connector](../cassandra_connector_phantom) (tables are defined in [UnitMessagesRepository](../examples/src/main/scala/com/examples/trips/UnitMessagesRepository.scala))

P.S. This saved a lot of time when developing over 30 kind of reports
