package com.github.trembita.akka_streams

import akka.stream.Materializer
import scala.language.higherKinds
import akka.stream.scaladsl._
import cats.effect.IO
import scala.annotation.implicitNotFound
import scala.concurrent.Future

@implicitNotFound("""
    Not found implicit value of type RunAkka[${F}].
    Probably Akka doesn't support using ${F} as a part of stream.
    For common cases with Futures or IO please ensure you have implicit Materializer and Parallelism defined in scope.
  """)
sealed trait RunAkka[F[_]] extends Serializable {
  def traverse[A, B, Mat](source: Source[A, Mat])(f: A => F[B]): Source[B, Mat]
  def lift[A](a: A): F[A]
  def traverse_[A, B, Mat](source: Source[A, Mat])(f: A => F[Unit]): F[Unit]
}

case class Parallelism(value: Int, ordered: Boolean)

trait LowPriorityParallelism {
  implicit lazy val processorsBasedUnOrdered: Parallelism =
    Parallelism(Runtime.getRuntime.availableProcessors(), ordered = false)
}

object Parallelism extends LowPriorityParallelism

class RunFutureOnAkka(parallelism: Parallelism)(implicit mat: Materializer) extends RunAkka[Future] {
  def traverse[A, B, Mat](
      source: Source[A, Mat]
  )(f: A => Future[B]): Source[B, Mat] =
    if (parallelism.ordered)
      source.mapAsync(parallelism.value)(f)
    else source.mapAsyncUnordered(parallelism.value)(f)

  def lift[A](a: A): Future[A] = Future.successful(a)

  def traverse_[A, B, Mat](
      source: Source[A, Mat]
  )(f: A => Future[Unit]): Future[Unit] =
    if (parallelism.ordered)
      source
        .mapAsync(parallelism.value)(f)
        .runWith(Sink.ignore)
        .map(_ => {})(mat.executionContext)
    else
      source
        .mapAsyncUnordered(parallelism.value)(f)
        .runWith(Sink.ignore)
        .map(_ => {})(mat.executionContext)
}

class RunIOOnAkka(parallelism: Parallelism)(implicit mat: Materializer) extends RunAkka[IO] {
  def traverse[A, B, Mat](
      source: Source[A, Mat]
  )(f: A => IO[B]): Source[B, Mat] =
    if (parallelism.ordered)
      source.mapAsync(parallelism.value)(f(_).unsafeToFuture())
    else source.mapAsyncUnordered(parallelism.value)(f(_).unsafeToFuture())

  def lift[A](a: A): IO[A] = IO.pure(a)

  def traverse_[A, B, Mat](source: Source[A, Mat])(f: A => IO[Unit]): IO[Unit] =
    IO.fromFuture(IO {
        if (parallelism.ordered)
          source
            .mapAsync(parallelism.value)(f(_).unsafeToFuture())
            .runWith(Sink.ignore)
        else
          source
            .mapAsync(parallelism.value)(f(_).unsafeToFuture())
            .runWith(Sink.ignore)
      })
      .map(_ => {})
}
