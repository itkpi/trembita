package com.github.trembita.util

import cats.effect.IO

import scala.annotation.implicitNotFound
import scala.concurrent.Future
import scala.language.higherKinds

@implicitNotFound("""
    Unable to create ${F} from scala.concurrent.Future
    Please provide an implicit instance in scope if necessary
    """)
trait LiftFuture[F[_]] {
  def apply[A](future: => Future[A]): F[A]
}

object LiftFuture {
  def apply[F[_]](implicit liftFuture: LiftFuture[F]): LiftFuture[F] = liftFuture

  implicit val liftFutureToIO: LiftFuture[IO] = new LiftFuture[IO] {
    def apply[A](future: => Future[A]): IO[A] = IO.fromFuture(IO.delay(future))
  }

  implicit val liftFutureToFuture: LiftFuture[Future] = new LiftFuture[Future] {
    override def apply[A](future: => Future[A]): Future[A] = future
  }
}
