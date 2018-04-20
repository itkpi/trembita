package com.datarootlabs.trembita.examples.trips

import java.time.LocalDateTime
import com.outworkers.phantom.connectors.ContactPoints
import com.datarootlabs.trembita.ql.show._
import com.datarootlabs.trembitazation.circe._
import io.circe.syntax._

object Main {
  def main(args: Array[String]): Unit = {
    val cassandraConnection = {
      val hosts = List("localhost")
      val keyspace = "trips"
      val username = "cassandra"
      val password = "cassandra"
      val port = 9042

      ContactPoints(hosts)
        .withClusterBuilder(_.withCredentials(username, password).withPort(port))
        .keySpace(keyspace)
    }

    val unitMessagesRepository = new UnitMessagesRepository(cassandraConnection)
    val sampleReport = new SampleReport(unitMessagesRepository)

    val unitIds = List("123", "456")
    val fromDate = LocalDateTime.now.minusMonths(1)
    val toDate = LocalDateTime.now

    val report = sampleReport.generate(unitIds, fromDate, toDate)

    println("----------- Result -----------")
    println(report.map(_.pretty()))

    println("\n\n------------ JSON -----------")
    println(report.map(_.asJson))
  }
}
