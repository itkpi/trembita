package com.github.trembita.operations
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait CanSlice[F[_]] {
  def slice[A](fa: F[A], from: Int, to: Int): F[A]
}

trait LowPriorityCanSlice {
  implicit def fromTakeAndDrop[F[_]](implicit canTake: CanTake[F],
                                     canDrop: CanDrop[F]): CanSlice[F] =
    new CanSlice[F] {
      def slice[A](fa: F[A], from: Int, to: Int): F[A] =
        canTake.take(canDrop.drop(fa, from), to - from)
    }
}

object CanSlice extends LowPriorityCanSlice {
  implicit val canSliceVector: CanSlice[Vector] = new CanSlice[Vector] {
    def slice[A](fa: Vector[A], from: Int, to: Int): Vector[A] =
      fa.slice(from, to)
  }

  implicit val canSliceParVector: CanSlice[ParVector] =
    new CanSlice[ParVector] {
      def slice[A](fa: ParVector[A], from: Int, to: Int): ParVector[A] =
        fa.slice(from, to)
    }
}
