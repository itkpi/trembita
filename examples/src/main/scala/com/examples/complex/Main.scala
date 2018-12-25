package com.examples.complex

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.effect.{ExitCode, IO, IOApp}
import com.github.trembita.DataPipelineT
import com.github.trembita.experimental.akka._
import org.apache.spark.sql.SparkSession
import cats.syntax.functor._
import com.github.trembita.caching._
import com.github.trembita.caching.infinispan.InfinispanDefaultCaching
import com.github.trembita.experimental.spark._
import scala.concurrent.duration._

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

          InfinispanDefaultCaching[IO, Spark, String](???, ExpirationTimeout(15.seconds)).bracket(use = {
            implicit caching: Caching[IO, Spark, String] =>
              akkaSparkExample
          })(release = _.stop())

        })(release = spark => IO(spark.stop()))

      })(release = system => IO.fromFuture(IO(system.terminate())).void)
      .as(ExitCode.Success)

  def akkaSparkExample(implicit mat: ActorMaterializer, spark: SparkSession, caching: Caching[IO, Spark, String]): IO[Unit] = {
    val csvPipeline: DataPipelineT[IO, Map[String, ByteString], Akka] =
      DataPipelineT.fromRepr[IO, Map[String, ByteString], Akka](
        Source
          .fromIterator(
            () => scala.io.Source.fromURL("https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD").getLines()
          )
          .map(ByteString(_))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMap())
      )

//    val recordsPipeline = csvPipeline.map

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
