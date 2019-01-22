package trembita.operations
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} does not support `take` operation natively.
    Please provide an implicit instance in scope if necessary
    """)
trait CanTake[F[_]] extends Serializable {
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
