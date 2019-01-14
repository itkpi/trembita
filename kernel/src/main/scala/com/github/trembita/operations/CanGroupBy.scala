package com.github.trembita.operations

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support groupBy operation or it is not efficient.
    If you want to groupBy ${F}, please provide an implicit instance in scope
  """)
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
