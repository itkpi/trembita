package com.examples.akka_spark

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
    val csvPipeline = DataPipelineT.fromRepr[IO, String, Akka](
      Source.fromIterator(() => scala.io.Source.fromURL("https://sample-videos.com/csv/Sample-Spreadsheet-500000-rows.csv").getLines())
    )


    ???
  }

  case class CsvRecord()
}
