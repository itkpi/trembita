package com.examples.trips

import java.time.LocalDateTime
import cats.effect.{ExitCode, IO, IOApp}
import com.outworkers.phantom.connectors.ContactPoints
import com.examples.putStrLn
import cats.implicits._
import com.github.trembita.ql.show._

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val cassandraConnection = IO {
      val hosts = List("localhost")
      val keyspace = "trips"
      val username = "cassandra"
      val password = "cassandra"
      val port = 9042

      ContactPoints(hosts)
        .withClusterBuilder(
          _.withCredentials(username, password).withPort(port)
        )
        .keySpace(keyspace)
    }

    val unitMessagesRepository =
      cassandraConnection.map(new UnitMessagesRepository(_))
    val sampleReport = unitMessagesRepository.map(new SampleReport(_))

    val unitIds = List("123", "456")
    val fromDate = LocalDateTime.now.minusMonths(1)
    val toDate = LocalDateTime.now

    val report = sampleReport.flatMap(_.generate(unitIds, fromDate, toDate))

    report
      .flatTap { report =>
        putStrLn("----------- Result -----------") *>
          putStrLn(report.map(_.pretty()))
      }
      .as(ExitCode.Success)
  }
}
