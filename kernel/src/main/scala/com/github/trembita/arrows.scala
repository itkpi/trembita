package com.github.trembita

import cats.arrow.FunctionK
import cats.effect.IO
import cats.{Applicative, Id, MonadError, ~>}
import cats.syntax.applicative._

import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

trait arrows {
  implicit def identityK[F[_]]: F ~> F = FunctionK.id

  implicit val futureToIO: Future ~> IO =
    λ[Future[?] ~> IO[?]](x => IO.fromFuture(IO { x }))

  implicit val ioToFuture: IO ~> Future =
    λ[IO[?] ~> Future[?]](_.unsafeToFuture)

  implicit val ioToTry: IO ~> Try =
    λ[IO[?] ~> Try[?]](ioa => Try { ioa.unsafeRunSync() })

  implicit def idToTry: Id ~> Try =
    λ[Id[?] ~> Try[?]](a => Success(a))

  implicit def idToEither: Id ~> Either[Throwable, ?] =
    λ[Id[?] ~> Either[Throwable, ?]](a => Right(a))

  implicit def idToFuture: Id ~> Future =
    λ[Id[?] ~> Future[?]](a => Future.successful(a))

  implicit def idToIO: Id ~> IO =
    λ[Id[?] ~> IO[?]](a => IO.pure(a))

  implicit def tryToFuture: Try ~> Future =
    λ[Try[?] ~> Future[?]](a => Future.fromTry(a))

  implicit def tryToIO: Try ~> IO =
    λ[Try[?] ~> IO[?]](a => IO.fromEither(a.toEither))

  implicit def eitherToTry: Either[Throwable, ?] ~> Try =
    λ[Either[Throwable, ?] ~> Try[?]](_.toTry)

  implicit def eitherToFuture: Either[Throwable, ?] ~> Future =
    λ[Either[Throwable, ?] ~> Future[?]](a => Future.fromTry(a.toTry))

  implicit def eitherToIO: Either[Throwable, ?] ~> IO =
    λ[Either[Throwable, ?] ~> IO[?]](IO.fromEither(_))
}
