package com.github.trembita

import cats.arrow.FunctionK
import cats.effect.IO
import cats.{~>, Applicative, Id, MonadError}
import cats.syntax.applicative._

import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

trait arrows {
  def identityK[F[_]]: F ~> F = FunctionK.id

  val futureToIO: Future ~> IO =
    λ[Future[?] ~> IO[?]](x => IO.fromFuture(IO { x }))

  val ioToFuture: IO ~> Future =
    λ[IO[?] ~> Future[?]](_.unsafeToFuture)

  val ioToTry: IO ~> Try =
    λ[IO[?] ~> Try[?]](ioa => Try { ioa.unsafeRunSync() })

  val idToTry: Id ~> Try =
    λ[Id[?] ~> Try[?]](a => Success(a))

  val idToEither: Id ~> Either[Throwable, ?] =
    λ[Id[?] ~> Either[Throwable, ?]](a => Right(a))

  val idToFuture: Id ~> Future =
    λ[Id[?] ~> Future[?]](a => Future.successful(a))

  val idToIO: Id ~> IO =
    λ[Id[?] ~> IO[?]](a => IO.pure(a))

  val tryToFuture: Try ~> Future =
    λ[Try[?] ~> Future[?]](a => Future.fromTry(a))

  val tryToIO: Try ~> IO =
    λ[Try[?] ~> IO[?]](
      a =>
        IO.fromEither(a match {
          case scala.util.Success(x) => Right(x)
          case scala.util.Failure(e) => Left(e)
        })
    )

  val eitherToTry: Either[Throwable, ?] ~> Try =
    λ[Either[Throwable, ?] ~> Try[?]] {
      case Left(e)  => scala.util.Failure(e)
      case Right(x) => scala.util.Success(x)
    }

  val eitherToFuture: Either[Throwable, ?] ~> Future =
    λ[Either[Throwable, ?] ~> Future[?]](
      a =>
        Future.fromTry(a match {
          case Left(e)  => scala.util.Failure(e)
          case Right(x) => scala.util.Success(x)
        })
    )

  val eitherToIO: Either[Throwable, ?] ~> IO =
    λ[Either[Throwable, ?] ~> IO[?]](IO.fromEither(_))
}
