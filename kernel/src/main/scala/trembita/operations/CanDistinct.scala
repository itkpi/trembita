package trembita.operations
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CanDistinct[F[_]] extends Serializable {
  def distinct[A: ClassTag](fa: F[A]): F[A]
}

object CanDistinct {
  implicit val canDistinctVector: CanDistinct[Vector]       = CanGroupBy.canGroupByVector
  implicit val canDistinctParVector: CanDistinct[ParVector] = CanGroupBy.canGroupByParVector
}