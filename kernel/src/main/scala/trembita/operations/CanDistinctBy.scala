package trembita.operations

import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CanDistinctBy[F[_]] extends CanDistinct[F] {
  def distinctBy[A: ClassTag, B: ClassTag](fa: F[A])(f: A => B): F[A]
  override def distinct[A: ClassTag](fa: F[A]): F[A] = distinctBy(fa)(identity)
}

object CanDistinctBy {
  implicit val canDistinctByVector: CanDistinctBy[Vector]       = CanGroupBy.canGroupByVector
  implicit val canDistinctByParVector: CanDistinctBy[ParVector] = CanGroupBy.canGroupByParVector
}
