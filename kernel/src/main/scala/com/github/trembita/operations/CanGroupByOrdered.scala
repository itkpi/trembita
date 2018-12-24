package com.github.trembita.operations

import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CanGroupByOrdered[F[_]] {
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
