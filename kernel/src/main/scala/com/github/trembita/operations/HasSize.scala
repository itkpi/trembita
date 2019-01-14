package com.github.trembita.operations

import cats.Id
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} does not provide an efficient way to calculate its size.
    Please provide an implicit instance in scope if necessary
    """)
trait HasSize[F[_]] {
  type Result[_]
  def size[A](fa: F[A]): Result[Int]
}

object HasSize {
  type Aux[F[_], R0[_]] = HasSize[F] { type Result[X] = R0[X] }

  implicit val vectorHasSize: HasSize.Aux[Vector, Id] = new HasSize[Vector] {
    type Result[X] = X
    def size[A](fa: Vector[A]): Int = fa.size
  }

  implicit val parVectorHasSize: HasSize.Aux[ParVector, Id] =
    new HasSize[ParVector] {
      type Result[X] = X
      def size[A](fa: ParVector[A]): Int = fa.size
    }
}
