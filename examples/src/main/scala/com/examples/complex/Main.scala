//package com.examples.complex
//
//import akka.NotUsed
//import akka.actor.ActorSystem
//import akka.stream.ActorMaterializer
//import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
//import akka.stream.scaladsl.Source
//import akka.util.ByteString
//import cats.effect.{ExitCode, IO, IOApp}
//import com.github.trembita._
//import com.github.trembita.experimental.akka._
//import org.apache.spark.sql.SparkSession
//import cats.syntax.functor._
//import cats.instances.future._
//import com.github.trembita.caching._
//import com.github.trembita.experimental.spark._
//import com.github.trembita.seamless.infinispan_spark._
//import com.github.trembita.seamless.akka_spark._
//import org.infinispan.client.hotrod.{RemoteCacheManager, Search}
//import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//import com.softwaremill.sttp._
//import com.softwaremill.sttp.asynchttpclient.cats.AsyncHttpClientCatsBackend
//import com.softwaremill.sttp.impl.cats._
//
//object Main extends IOApp {
//  def akkaSparkExample(implicit mat: ActorMaterializer, spark: SparkSession, caching: Caching[IO, Spark, String]): IO[Unit] = {
//    implicit val seamlessAkkaToSpark        = akkaToSpark[NotUsed](bufferLimit = 1000)
//    implicit val asyncTimeout: AsyncTimeout = AsyncTimeout(5.minutes)
//
//    val csvPipeline: DataPipelineT[Future, Map[String, ByteString], Akka] =
//      DataPipelineT.fromRepr[Future, Map[String, ByteString], Akka](
//        Source
//          .fromIterator(
//            () => scala.io.Source.fromURL("https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD").getLines()
//          )
//          .map(ByteString(_))
//          .via(CsvParsing.lineScanner())
//          .via(CsvToMap.toMap())
//      )
//
//    val recordsPipeline: DataPipelineT[Future, CsvRecord, Akka] = csvPipeline.map { byteStringFields: Map[String, ByteString] =>
//      val fields = byteStringFields.mapValues(_.utf8String)
//      CsvRecord(
//        fields("ID").toLong,
//        fields("Case Number"),
//        fields("Date"),
//        fields("Block"),
//        fields("IUCR"),
//        fields("Primary Type"),
//        fields("Description"),
//        fields("Location Description"),
//        fields("Arrest").toLowerCase.toBoolean,
//        fields("Domestic").toLowerCase.toBoolean,
//        fields("Beat"),
//        fields("District"),
//        fields("Ward"),
//        fields("Community Area"),
//        fields("FBI Code"),
//        toOpt(fields get "X Coordinate")(_.toLong),
//        toOpt(fields get "Y Coordinate")(_.toLong),
//        fields("Year").toInt,
//        fields("Updated On"),
//        toOpt(fields get "Latitude")(_.toDouble),
//        toOpt(fields get "Longitude")(_.toDouble),
//        fields.get("Location").filter(_.nonEmpty)
//      )
//    }
//
//    val atSpark = recordsPipeline
//      .toF[Spark]
//      .mapK(futureToIO)
////
////    val withAddress: DataPipelineT[IO, CsvRecord, Spark] = atSpark.mapM {csvRecord =>
////      implicit val backend = new AsyncHttpClientCatsBackend(new AsyncHttpBackend)
////    }
//    ???
//  }
//
//  private def toOpt[A](field: Option[String])(f: String => A): Option[A] =
//    field.filter(_.nonEmpty).map(f)
//
//  def run(args: List[String]): IO[ExitCode] =
//    IO(ActorSystem("trembita-system"))
//      .bracket(use = { implicit system: ActorSystem =>
//        implicit val mat = ActorMaterializer()
//
//        IO(
//          SparkSession
//            .builder()
//            .master("spark://spark-master:7077")
//            .appName("trembita-spark")
//            .getOrCreate()
//        ).bracket(use = {
//          implicit spark: SparkSession =>
//            IO(
//              new RemoteCacheManager(
//                new ConfigurationBuilder()
//                  .addServer()
//                  .host("localhost")
//                  .port(11222)
//                  .security()
//                  .authentication()
//                  .username("trembita")
//                  .password("trembita")
//                  .build()
//              )
//            ).bracket(use = cacheManager => {
//              cacheManager.start()
//              val cacheIO        = IO(cacheManager.getCache[String, Vector[String]]("trembita-spark-cache"))
//              val queryFactoryIO = cacheIO.flatMap(cache => IO(Search.getQueryFactory(cache)))
//              InfinispanPartitionCaching[IO, String](
//                cacheIO,
//                queryFactoryIO,
//                ExpirationTimeout(15.seconds),
//                spark.sparkContext
//              ).bracket(use = { implicit caching: Caching[IO, Spark, String] =>
//                akkaSparkExample
//              })(release = _.stop())
//            })(release = cacheManager => IO(cacheManager.stop()))
//
//        })(release = spark => IO(spark.stop()))
//
//      })(release = system => IO.fromFuture(IO(system.terminate())).void)
//      .as(ExitCode.Success)
//}
