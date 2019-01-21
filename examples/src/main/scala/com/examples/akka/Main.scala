package com.examples.akka

import java.nio.file.Paths

import akka.NotUsed
import cats.effect._
import cats.syntax.functor._
import com.github.trembita._
import cats.effect.Console.io._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import com.github.trembita.akka.{Akka, Parallelism}
import com.github.trembita.experimental.akka._

import scala.concurrent.ExecutionContext

object Main extends IOApp {
  def akkaSample(implicit mat: ActorMaterializer, ec: ExecutionContext): IO[Unit] = {

    implicit val parallelism: Parallelism = Parallelism(8, ordered = false)

    val fileLines =
      DataPipelineT
        .fromReprF[IO, ByteString, Akka[NotUsed]](IO {
          FileIO
            .fromPath(Paths.get(getClass.getResource("/words.txt").toURI))
            .mapMaterializedValue(_ => NotUsed)
        })

    val wordsCount: DataPipelineT[IO, String, Akka[NotUsed]] = fileLines
      .map(_.utf8String)
      .mapConcat(_.split("\\s"))
      .groupBy(identity _)
      .mapValues(_.size)
      .map { case (word, count) => s"`$word` occurs $count times" }
      .mapRepr(
        _.intersperse("\n") /* function called directly on stream */
      )

    wordsCount
      .runForeachF(putStrLn)
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
