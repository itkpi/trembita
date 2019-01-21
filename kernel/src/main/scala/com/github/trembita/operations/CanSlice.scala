package com.github.trembita.operations
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} does not support `slice` operation natively.
    Please provide an implicit instance in scope if necessary
    """)
trait CanSlice[F[_]] extends Serializable {
  def slice[A](fa: F[A], from: Int, to: Int): F[A]
}

trait LowPriorityCanSlice extends Serializable {
  implicit def fromTakeAndDrop[F[_]](implicit canTake: CanTake[F], canDrop: CanDrop[F]): CanSlice[F] =
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
