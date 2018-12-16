package com.github.trembita

import cats.arrow.FunctionK
import cats.effect.IO
import cats.{Applicative, Id, MonadError, ~>}
import cats.syntax.applicative._

import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

trait arrows {
  implicit def tryTo[G[_]](implicit G: MonadError[G, Throwable]): Try ~> G =
    λ[Try[?] ~> G[?]](x => G.fromTry(x))

  implicit def identityK[F[_]]: F ~> F = FunctionK.id

  implicit val futureToIO: Future ~> IO =
    λ[Future[?] ~> IO[?]](x => IO.fromFuture(IO { x }))

  implicit val ioToFuture: IO ~> Future =
    λ[IO[?] ~> Future[?]](_.unsafeToFuture)

  implicit val ioToTry: IO ~> Try =
    λ[IO[?] ~> Try[?]](ioa => Try { ioa.unsafeRunSync() })

  implicit def idToAnyApplicative[F[_]: Applicative]: Id ~> F =
    λ[Id[?] ~> F[?]](_.pure[F])
}
