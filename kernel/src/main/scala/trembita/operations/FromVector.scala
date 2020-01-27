package trembita.operations

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} cannot be created from scala.Vector.
    Please provide an implicit instance in scope if necessary
    """)
trait FromVector[F[_]] extends Serializable {
  def apply[A](vs: Vector[A]): F[A]
}

object FromVector {
  implicit val vectorFromVector: FromVector[Vector] = new FromVector[Vector] {
    def apply[A](vs: Vector[A]): Vector[A] = vs
  }

  implicit val parVectorFromVector: FromVector[ParVector] = new FromVector[ParVector] {
    def apply[A](vs: Vector[A]): ParVector[A] = ParVector(vs:_*)
  }
}
