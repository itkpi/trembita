package com.examples.akka

import java.nio.file.Paths
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import cats.effect.Console.io._
import cats.effect._
import cats.syntax.functor._
import trembita._
import trembita.akka_streams._
import scala.concurrent.{ExecutionContext, Future}

object Main extends IOApp {
  def akkaSample(implicit mat: ActorMaterializer, ec: ExecutionContext): IO[Unit] = {

    implicit val parallelism: Parallelism = Parallelism(8, ordered = false)

    val fileLines =
      Input.fromSourceF[IO](
        FileIO
          .fromPath(Paths.get("examples/src/main/resources/words.txt"))
      )

    val wordsCount: BiDataPipelineT[IO, String, Akka[Future[IOResult]]] = fileLines
      .map(_.utf8String)
      .mapConcat(_.split("\\s"))
      .groupByKey(identity _)
      .mapValues(_.size)
      .map { case (word, count) => s"`$word` occurs $count times" }
      .mapRepr(
        _.intersperse("\n") /* function called directly on stream */
      )

    wordsCount
      .into(Output.collection[Vector])
      .run
      .flatMap(res => putStrLn(res.mkString("")))
  }

  def run(args: List[String]): IO[ExitCode] =
    IO {
      ActorSystem("trembita-akka")
    }.bracket(use = { implicit system: ActorSystem =>
        akkaSample(ActorMaterializer(), system.dispatcher)
      })(
        release = system =>
          IO.fromFuture(IO {
              system.terminate()
            })
            .void
      )
      .as(ExitCode.Success)
}
