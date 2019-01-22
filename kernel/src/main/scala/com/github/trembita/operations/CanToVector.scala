package com.github.trembita.operations

import cats.Id
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} does not support conversion to scala.Vector natively.
    Please provide an implicit instance in scope if necessary
    """)
trait CanToVector[F[_]] extends Serializable {
  type Result[X]
  def apply[A](fa: F[A]): Result[Vector[A]]
}

object CanToVector {
  type Aux[F[_], R0[_]] = CanToVector[F] { type Result[X] = R0[X] }
  implicit val vectorToVector: CanToVector.Aux[Vector, Id] = new CanToVector[Vector] {
    final type Result[X] = X
    def apply[A](fa: Vector[A]): Vector[A] = fa
  }

  implicit val parVectorToVector: CanToVector.Aux[ParVector, Id] = new CanToVector[ParVector] {
    final type Result[X] = X
    def apply[A](fa: ParVector[A]): Vector[A] = fa.seq
  }
}
