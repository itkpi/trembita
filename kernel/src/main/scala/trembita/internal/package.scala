package trembita

import zio.{IO, UIO}

package object internal {
  implicit class WidenIO[E, A](private val self: IO[E, A]) extends AnyVal {
    def widen[EE >: E, B >: A]: IO[EE, B] = self
  }
  implicit class WidenEitherTUIO[F[_], E, A](private val self: UIO[F[Either[E, A]]]) extends AnyVal {
    def widenEither[F2[_], EE >: E, B >: A]: UIO[F2[Either[EE, B]]] = self.asInstanceOf[UIO[F2[Either[EE, B]]]]
  }

  def eitherOrdering[E, A: Ordering]: Ordering[Either[E, A]] =
    (x: Either[E, A], y: Either[E, A]) =>
      (x, y) match {
        case (Left(_), Left(_))     => 0
        case (Left(_), Right(_))    => -1
        case (Right(_), Left(_))    => 1
        case (Right(v1), Right(v2)) => Ordering[A].compare(v1, v2)
    }
}
