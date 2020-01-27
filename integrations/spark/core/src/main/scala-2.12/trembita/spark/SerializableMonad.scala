package trembita.spark

import cats.effect.IO
import cats.{CoflatMap, Eval, Id, Monad, MonadError, StackSafeMonad}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait SerializableMonad[F[_]] extends Monad[F] with Serializable

object SerializableMonad {
  implicit val idMonad: SerializableMonad[Id] = new SerializableMonad[Id] {
    def pure[A](x: A): Id[A]                           = x
    def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] = f(fa)
    @tailrec def tailRecM[A, B](a: A)(f: A => Either[A, B]): B = f(a) match {
      case Left(a1) => tailRecM(a1)(f)
      case Right(b) => b
    }
  }
}

trait SerializableMonadError[F[_]] extends MonadError[F, Throwable] with Serializable

trait SerializableCoflatMap[F[_]] extends CoflatMap[F] with Serializable

trait LowPriorityInstancesForSpark extends Serializable {
  @transient lazy implicit val safeIOInstances: SerializableMonadError[IO] with SerializableCoflatMap[IO] with SerializableMonad[IO] =
    new SerializableMonadError[IO] with SerializableCoflatMap[IO] with SerializableMonad[IO] with StackSafeMonad[IO] {
      def pure[A](x: A): IO[A] = IO.pure(x)

      def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] =
        fa.flatMap(f)

      def handleErrorWith[A](fea: IO[A])(
          f: Throwable => IO[A]
      ): IO[A] = fea.handleErrorWith(f)

      def raiseError[A](e: Throwable): IO[A] = IO.raiseError(e)
      override def handleError[A](fea: IO[A])(
          f: Throwable => A
      ): IO[A]                                            = fea.handleErrorWith(t => IO.pure(f(t)))
      override def map[A, B](fa: IO[A])(f: A => B): IO[B] = fa.map(f)

      def coflatMap[A, B](fa: IO[A])(f: IO[A] => B): IO[B] =
        IO(f(fa))
    }

  @transient lazy implicit val safeIdInstances: SerializableCoflatMap[Id] with SerializableMonad[Id] =
    new SerializableCoflatMap[Id] with SerializableMonad[Id] with StackSafeMonad[Id] {
      def pure[A](x: A): Id[A] = x

      def flatMap[A, B](fa: Id[A])(f: A => Id[B]): Id[B] =
        f(fa)

      override def map[A, B](fa: Id[A])(f: A => B): Id[B] = f(fa)

      def coflatMap[A, B](fa: Id[A])(f: Id[A] => B): Id[B] =
        f(fa)
    }

  @transient lazy implicit val safeTryInstances: SerializableMonadError[Try] with SerializableCoflatMap[Try] with SerializableMonad[Try] =
    new SerializableMonadError[Try] with SerializableCoflatMap[Try] with SerializableMonad[Try] with StackSafeMonad[Try] {
      def pure[A](x: A): Try[A] = Success(x)

      def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] =
        fa.flatMap(f)

      def handleErrorWith[A](fea: Try[A])(
          f: Throwable => Try[A]
      ): Try[A] = fea.recoverWith { case t => f(t) }

      def raiseError[A](e: Throwable): Try[A] = Failure(e)
      override def handleError[A](fea: Try[A])(
          f: Throwable => A
      ): Try[A]                                             = fea.recover { case t => f(t) }
      override def map[A, B](fa: Try[A])(f: A => B): Try[B] = fa.map(f)

      def coflatMap[A, B](fa: Try[A])(f: Try[A] => B): Try[B] =
        Try(f(fa))
    }

  @transient lazy implicit val safeEitherInstances: SerializableMonadError[Either[Throwable, ?]]
    with SerializableCoflatMap[Either[Throwable, ?]]
    with SerializableMonad[Either[Throwable, ?]] =
    new SerializableMonadError[Either[Throwable, ?]] with SerializableCoflatMap[Either[Throwable, ?]]
    with SerializableMonad[Either[Throwable, ?]] with StackSafeMonad[Either[Throwable, ?]] {
      def pure[A](x: A): Either[Throwable, A]                                                           = Right(x)
      def coflatMap[A, B](fa: Either[Throwable, A])(f: Either[Throwable, A] => B): Either[Throwable, B] = Right(f(fa))

      def flatMap[A, B](fa: Either[Throwable, A])(f: A => Either[Throwable, B]): Either[Throwable, B] = fa.right.flatMap(f)
      def raiseError[A](e: Throwable): Either[Throwable, A]                                           = Left[Throwable, A](e)
      def handleErrorWith[A](fa: Either[Throwable, A])(f: Throwable => Either[Throwable, A]): Either[Throwable, A] =
        if (fa.isRight) fa
        else f(fa.left.get)
    }

}
