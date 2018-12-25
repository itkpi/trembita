package com.github.trembita.operations
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait FromVector[F[_]] {
  def apply[A](vs: Vector[A]): F[A]
}

object FromVector {
  implicit val vectorFromVector: FromVector[Vector] = new FromVector[Vector] {
    def apply[A](vs: Vector[A]): Vector[A] = vs
  }

  implicit val parVectorFromVector: FromVector[ParVector] = new FromVector[ParVector] {
    def apply[A](vs: Vector[A]): ParVector[A] = vs.par
  }
}
