package com.github.trembita.operations
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait CanDrop[F[_]] {
  def drop[A](fa: F[A], n: Int): F[A]
}

object CanDrop {
  implicit val canDropVector: CanDrop[Vector] = new CanDrop[Vector] {
    def drop[A](fa: Vector[A], n: Int): Vector[A] = fa.drop(n)
  }
  implicit val canDropParVector: CanDrop[ParVector] = new CanDrop[ParVector] {
    def drop[A](fa: ParVector[A], n: Int): ParVector[A] = fa.drop(n)
  }
}
