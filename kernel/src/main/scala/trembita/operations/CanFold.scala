package trembita.operations

import cats.Id

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support fold operation or it is not efficient.
    If you want to fold ${F}, please provide an implicit instance in scope
  """)
trait CanFold[F[_]] extends CanReduce[F] {
  type Result[_]

  def fold[A: ClassTag](fa: F[A])(zero: A)(f: (A, A) => A): Result[A] =
    foldLeft(fa)(zero)(f)

  def foldLeft[A: ClassTag, B: ClassTag](fa: F[A])(zero: B)(
      f: (B, A) => B
  ): Result[B]
}

object CanFold {
  type Aux[F[_], R0[_]] = CanFold[F] { type Result[X] = R0[X] }

  implicit val canFoldVector: CanFold.Aux[Vector, Id] = new CanFold[Vector] {
    type Result[X] = X

    def foldLeft[A: ClassTag, B: ClassTag](fa: Vector[A])(zero: B)(
        f: (B, A) => B
    ): B = fa.foldLeft[B](zero)(f)

    def reduceOpt[A: ClassTag](
        fa: Vector[A]
    )(f: (A, A) => A): Result[Option[A]] =
      fa.reduceOption(f)

    def reduce[A: ClassTag](fa: Vector[A])(f: (A, A) => A): Result[A] =
      fa.reduce(f)
  }

  implicit val canFoldParVector: CanFold.Aux[ParVector, Id] =
    new CanFold[ParVector] {
      type Result[X] = X

      def foldLeft[A: ClassTag, B: ClassTag](fa: ParVector[A])(zero: B)(
          f: (B, A) => B
      ): B = fa.foldLeft(zero)(f)

      def reduceOpt[A: ClassTag](
          fa: ParVector[A]
      )(f: (A, A) => A): Result[Option[A]] =
        fa.reduceOption(f)

      def reduce[A: ClassTag](fa: ParVector[A])(f: (A, A) => A): Result[A] =
        fa.reduce(f)
    }
}
