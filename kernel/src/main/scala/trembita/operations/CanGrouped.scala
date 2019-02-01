package trembita.operations
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait CanGrouped[F[_]] {
  def grouped[A](fa: F[A], n: Int): F[Iterable[A]]
}

object CanGrouped {
  implicit val vectorCanBeGrouped: CanGrouped[Vector] = new CanGrouped[Vector] {
    def grouped[A](fa: Vector[A], n: Int): Vector[Iterable[A]] = fa.grouped(n).toVector
  }
  implicit val parVectorCanBeGrouped: CanGrouped[ParVector] = new CanGrouped[ParVector] {
    def grouped[A](fa: ParVector[A], n: Int): ParVector[Iterable[A]] = fa.seq.grouped(n).toVector.par
  }
}
