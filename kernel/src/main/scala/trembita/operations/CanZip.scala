package trembita.operations

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support `zip` operation natively.
    Please provide an implicit instance in scope if necessary
    """)
trait CanZip[F[_]] extends Serializable {
  def zip[A: ClassTag, B: ClassTag](fa: F[A], fb: F[B]): F[(A, B)]
}

object CanZip {
  implicit val canZipVectors: CanZip[Vector] = new CanZip[Vector] {
    def zip[A: ClassTag, B: ClassTag](fa: Vector[A], fb: Vector[B]): Vector[(A, B)] = fa zip fb
  }

  implicit val canZipParVectors: CanZip[ParVector] = new CanZip[ParVector] {
    def zip[A: ClassTag, B: ClassTag](fa: ParVector[A], fb: ParVector[B]): ParVector[(A, B)] = fa zip fb
  }
}
