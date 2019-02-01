package trembita.operations

import trembita.internal.BatchUtils
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds

trait CanBatched[F[_]] {
  def batched[A](fa: F[A], parts: Int): F[Iterable[A]]
}

object CanBatched {
  implicit val vectorCanBeBatched: CanBatched[Vector] = new CanBatched[Vector] {
    def batched[A](fa: Vector[A], parts: Int): Vector[Iterable[A]] = BatchUtils.batch(parts)(fa).toVector
  }
  implicit val parVectorCanBeCatched: CanBatched[ParVector] = new CanBatched[ParVector] {
    def batched[A](fa: ParVector[A], parts: Int): ParVector[Iterable[A]] = BatchUtils.batch(parts)(fa.seq).toVector.par
  }
}
