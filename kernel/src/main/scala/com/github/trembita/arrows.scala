package com.github.trembita
import cats.arrow.FunctionK
import cats.effect.IO
import cats.{MonadError, ~>}
import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

trait arrows {
  implicit def TryT[G[_]](implicit G: MonadError[G, Throwable]): Try ~> G =
    λ[Try[?] ~> G[?]](x => G.fromTry(x))

  implicit def IdT[F[_]]: F ~> F = FunctionK.id

  implicit val FutureToIO: Future ~> IO =
    λ[Future[?] ~> IO[?]](x => IO.fromFuture(IO { x }))

  implicit val IO_ToFuture: IO ~> Future =
    λ[IO[?] ~> Future[?]](_.unsafeToFuture)

  implicit val IO2Try: IO ~> Try =
    λ[IO[?] ~> Try[?]](ioa => Try { ioa.unsafeRunSync() })
}
