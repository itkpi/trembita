package com.github.trembita.operations

import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CanGroupBy[F[_]] {
  def groupBy[K: ClassTag, V: ClassTag](fa: F[V])(f: V => K): F[(K, Iterable[V])]
}

object CanGroupBy {
  implicit val canGroupByVector: CanGroupBy[Vector] = new CanGroupBy[Vector] {
    def groupBy[K: ClassTag, V: ClassTag](fa: Vector[V])(f: V => K): Vector[(K, Iterable[V])] =
      fa.groupBy(f).toVector
  }

  implicit val canGroupByParVector: CanGroupBy[ParVector] = new CanGroupBy[ParVector] {
    def groupBy[K: ClassTag, V: ClassTag](fa: ParVector[V])(f: V => K): ParVector[(K, Iterable[V])] =
      fa.groupBy(f).mapValues(_.seq).toVector.par
  }
}
