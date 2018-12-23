package com.examples.akka_spark

import java.time.LocalDate
import akka.stream.alpakka.csv.scaladsl.CsvToMap
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import com.github.trembita._
import com.github.trembita.experimental.akka._
import com.github.trembita.experimental.spark._
import com.github.trembita.seamless.akka_spark._
import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.Console.io._
import cats.implicits._
import org.apache.spark.sql.SparkSession
import akka.stream.scaladsl._
import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import akka.util.ByteString

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    IO(ActorSystem("trembita-system"))
      .bracket(use = { implicit system: ActorSystem =>
        implicit val mat = ActorMaterializer()

        IO(
          SparkSession
            .builder()
            .master("spark://spark-master:7077")
            .appName("trembita-spark")
            .getOrCreate()
        ).bracket(use = { implicit spark: SparkSession =>
          akkaSparkExample
        })(release = spark => IO(spark.stop()))
      })(release = system => IO.fromFuture(IO(system.terminate())).void)
      .as(ExitCode.Success)

  def akkaSparkExample(implicit mat: ActorMaterializer, spark: SparkSession): IO[Unit] = {
    val csvPipeline = DataPipelineT.fromRepr[IO, Map[String, ByteString], Akka](
      Source
        .fromIterator(
          () => scala.io.Source.fromURL("https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD").getLines()
        )
        .map(ByteString(_))
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.toMap())
    )

    val recordsPipeline = csvPipeline.map {record =>
      record.toString
    }



    ???
  }

  case class CsvRecord(id: Long,
                       caseNumber: String,
                       date: String,
                       block: String,
                       IUCR: String,
                       primaryType: String,
                       description: String,
                       locationDescription: String,
                       arrest: Boolean,
                       domestic: Boolean,
                       beat: String,
                       district: String,
                       ward: String,
                       communityArea: String,
                       FBICode: String,
                       xCoordinate: Long,
                       yCoordinate: Long,
                       year: Int,
                       updatedOn: String,
                       latitude: Double,
                       longitude: Double,
                       location: String)
}
