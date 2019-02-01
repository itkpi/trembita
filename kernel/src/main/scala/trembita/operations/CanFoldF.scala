package trembita.operations

import cats.Monad
import cats.syntax.all._
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait CanFoldF[F[_], G[_]] {
  def foldF[A, B](fa: F[A])(zero: B)(f: (B, A) => G[B]): G[B]
}

object CanFoldF {
  implicit def canFoldFVector[F[_]: Monad]: CanFoldF[Vector, F] = new CanFoldF[Vector, F] {
    def foldF[A, B](fa: Vector[A])(zero: B)(f: (B, A) => F[B]): F[B] =
      fa.foldLeft(zero.pure[F])(
        (gb, a) =>
          gb.flatMap { b =>
            f(b, a)
        }
      )
  }

  implicit def canFoldFParVector[F[_]: Monad]: CanFoldF[ParVector, F] = new CanFoldF[ParVector, F] {
    def foldF[A, B](fa: ParVector[A])(zero: B)(f: (B, A) => F[B]): F[B] =
      fa.foldLeft(zero.pure[F])(
        (gb, a) =>
          gb.flatMap { b =>
            f(b, a)
        }
      )
  }
}
