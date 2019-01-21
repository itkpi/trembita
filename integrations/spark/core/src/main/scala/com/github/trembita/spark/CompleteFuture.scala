package com.github.trembita.spark

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReadWriteLock

import cats.effect.IO

import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.language.{higherKinds, implicitConversions}

sealed abstract class SerializableFutureNewType {
  private[trembita] trait Tag

  type NewType[A] <: Future[A] with Serializable with Tag
}

trait FutureIsSerializable[A] {
  def apply(): SerializableFutureImpl.NewType[A]
}

object FutureIsSerializable {
  implicit def materialize[A](fa: Future[A]): FutureIsSerializable[A] = macro rewrite.materializeSerializableFuture[A]
}

object SerializableFutureImpl extends SerializableFutureNewType {
  final type NewType[+A] = Future[A] with Serializable with Tag

  def lift[A](fa: FutureIsSerializable[A]): NewType[A] = fa()
  def pure[A](a: A): NewType[A]                        = new CompleteFuture[A](Success(a)).asInstanceOf[NewType[A]]
  def raiseError[A](e: Throwable): NewType[A]          = fromTry(Failure(e))
  def fromTry[A](fa: Try[A]): NewType[A]               = new CompleteFuture[A](fa).asInstanceOf[NewType[A]]
  def start[A](thunk: => A)(implicit ec: ExecutionContext): NewType[A] =
    new StartedFuture[A](() => Try { thunk })(ec).asInstanceOf[NewType[A]]

  private[trembita] def widen[A](fa: Future[A]): NewType[A] = fa.asInstanceOf[NewType[A]]
}

class CompleteFuture[A](_value: Try[A]) extends Future[A] with Serializable {
  def onComplete[U](f: Try[A] => U)(implicit executor: ExecutionContext): Unit =
    executor.execute(new Runnable { def run(): Unit = f(_value) })

  def isCompleted: Boolean = true

  def value: Option[Try[A]] = Some(_value)

  def transform[S](f: Try[A] => Try[S])(
      implicit executor: ExecutionContext
  ): Future[S] = {
    val p = Promise[S]
    executor.execute(new Runnable {
      def run(): Unit = {
        val res = f(_value)
        p.complete(res)
      }
    })
    p.future
  }

  def transformWith[S](f: Try[A] => Future[S])(
      implicit executor: ExecutionContext
  ): Future[S] = {
    val p = Promise[S]
    executor.execute(new Runnable {
      def run(): Unit = {
        val res = f(_value)
        p.completeWith(res)
      }
    })
    p.future
  }

  def ready(atMost: Duration)(
      implicit permit: CanAwait
  ): CompleteFuture.this.type = this

  def result(atMost: Duration)(implicit permit: CanAwait): A = _value.get
}

class StartedFuture[A](thunk: () => Try[A])(implicit ec: ExecutionContext) extends Future[A] with Serializable {
  private val result: Future[A] = IO
    .async[A] { cb =>
      ec.execute(new Runnable {
        def run(): Unit = {
          val result = Try { thunk() }.flatten
          cb(tryToEither(result))
        }
      })
    }
    .unsafeToFuture()

  def onComplete[U](f: Try[A] => U)(implicit executor: ExecutionContext): Unit =
    result.onComplete(f)(executor)

  def isCompleted: Boolean  = result.isCompleted
  def value: Option[Try[A]] = result.value

  def transform[S](f: Try[A] => Try[S])(
      implicit executor: ExecutionContext
  ): Future[S] =
    IO.async[S] { cb =>
        result.onComplete { fa =>
          val fs = f(fa)
          cb(tryToEither(fs))
        }(executor)
      }
      .unsafeToFuture()

  def transformWith[S](f: Try[A] => Future[S])(
      implicit executor: ExecutionContext
  ): Future[S] =
    IO.async[S] { cb =>
        result.onComplete { fa =>
          f(fa).onComplete { fs =>
            cb(tryToEither(fs))
          }(executor)
        }(executor)
      }
      .unsafeToFuture()

  def ready(atMost: Duration)(implicit permit: CanAwait): StartedFuture.this.type = {
    result.ready(atMost)
    this
  }

  def result(atMost: Duration)(implicit permit: CanAwait): A =
    result.result(atMost)

  private def tryToEither[x](fx: Try[x]): Either[Throwable, x] = fx match {
    case Failure(e) => Left(e)
    case Success(x) => Right(x)
  }
}
