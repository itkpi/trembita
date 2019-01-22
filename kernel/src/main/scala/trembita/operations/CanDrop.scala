package trembita.operations
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

@implicitNotFound("""
    ${F} does not support `drop` operation natively.
    Please provide an implicit instance in scope if necessary
    """)
trait CanDrop[F[_]] extends Serializable {
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
