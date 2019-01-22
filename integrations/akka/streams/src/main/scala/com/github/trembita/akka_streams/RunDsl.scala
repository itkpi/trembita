package com.github.trembita.akka_streams

import akka.stream.Materializer
import cats.effect.IO

import scala.concurrent.Future
import scala.language.higherKinds

abstract class RunDsl[F[_]] {
  def toRunAkka(parallelism: Parallelism): RunAkka[F]
}

object RunDsl {
  implicit def runFutureDsl(implicit mat: Materializer): RunDsl[Future] =
    new RunDsl[Future]() {
      def toRunAkka(parallelism: Parallelism): RunAkka[Future] =
        new RunFutureOnAkka(parallelism)
    }

  implicit def runIODsl(implicit mat: Materializer): RunDsl[IO] =
    new RunDsl[IO]() {
      def toRunAkka(parallelism: Parallelism): RunAkka[IO] =
        new RunIOOnAkka(parallelism)
    }
}
