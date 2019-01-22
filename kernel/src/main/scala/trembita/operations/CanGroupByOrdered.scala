package trembita.operations

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support groupByOrdered operation or it is not efficient.
    If you want to groupByOrdered ${F}, please provide an implicit instance in scope
  """)
trait CanGroupByOrdered[F[_]] extends Serializable {
  def groupBy[K: ClassTag: Ordering, V: ClassTag](fa: F[V])(f: V => K): F[(K, Iterable[V])]
}

object CanGroupByOrdered {
  implicit val canGroupByVector: CanGroupByOrdered[Vector] = new CanGroupByOrdered[Vector] {
    def groupBy[K: ClassTag: Ordering, V: ClassTag](fa: Vector[V])(f: V => K): Vector[(K, Iterable[V])] =
      fa.groupBy(f).toVector.sortBy(_._1)
  }

  implicit val canGroupByParVector: CanGroupByOrdered[ParVector] = new CanGroupByOrdered[ParVector] {
    def groupBy[K: ClassTag: Ordering, V: ClassTag](fa: ParVector[V])(f: V => K): ParVector[(K, Iterable[V])] =
      fa.groupBy(f).mapValues(_.seq).toVector.sortBy(_._1).par
  }
}
