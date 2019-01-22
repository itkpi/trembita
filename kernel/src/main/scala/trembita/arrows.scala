package trembita

import cats.arrow.FunctionK
import cats.effect.IO
import cats.{~>, Applicative, Id}
import scala.concurrent.Future
import scala.language.higherKinds
import scala.util.Try

trait arrows {
  @inline implicit def identityK[F[_]]: F ~> F = FunctionK.id

  implicit val futureToIO: Future ~> IO =
    λ[Future[?] ~> IO[?]](x => IO.fromFuture(IO { x }))

  implicit val ioToFuture: IO ~> Future =
    λ[IO[?] ~> Future[?]](_.unsafeToFuture)

  implicit val ioToTry: IO ~> Try =
    λ[IO[?] ~> Try[?]](ioa => Try { ioa.unsafeRunSync() })

  @inline implicit def idTo[F[_]](implicit F: Applicative[F]): Id ~> F =
    λ[Id[?] ~> F[?]](a => F.pure(a))

  implicit val tryToFuture: Try ~> Future =
    λ[Try[?] ~> Future[?]](a => Future.fromTry(a))

  implicit val tryToIO: Try ~> IO =
    λ[Try[?] ~> IO[?]](
      a =>
        IO.fromEither(a match {
          case scala.util.Success(x) => Right(x)
          case scala.util.Failure(e) => Left(e)
        })
    )

  implicit val eitherToTry: Either[Throwable, ?] ~> Try =
    λ[Either[Throwable, ?] ~> Try[?]] {
      case Left(e)  => scala.util.Failure(e)
      case Right(x) => scala.util.Success(x)
    }

  implicit val eitherToFuture: Either[Throwable, ?] ~> Future =
    λ[Either[Throwable, ?] ~> Future[?]](
      a =>
        Future.fromTry(a match {
          case Left(e)  => scala.util.Failure(e)
          case Right(x) => scala.util.Success(x)
        })
    )

  implicit val eitherToIO: Either[Throwable, ?] ~> IO =
    λ[Either[Throwable, ?] ~> IO[?]](IO.fromEither(_))
}
