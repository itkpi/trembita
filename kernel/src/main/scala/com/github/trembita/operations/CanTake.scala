package com.github.trembita.operations
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait CanTake[F[_]] {
  def take[A](fa: F[A], n: Int): F[A]
}

object CanTake {
  implicit val canTakeVector: CanTake[Vector] = new CanTake[Vector] {
    def take[A](fa: Vector[A], n: Int): Vector[A] = fa.take(n)
  }
  implicit val canTakeParVector: CanTake[ParVector] = new CanTake[ParVector] {
    def take[A](fa: ParVector[A], n: Int): ParVector[A] = fa.take(n)
  }
}
