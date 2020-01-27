package trembita.operations

import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support groupBy operation or it is not efficient.
    If you want to groupBy ${F}, please provide an implicit instance in scope
  """)
trait CanGroupBy[F[_]] extends CanDistinctBy[F] {
  def groupBy[K: ClassTag, V: ClassTag](fa: F[V])(f: V => K): F[(K, Iterable[V])]
}

object CanGroupBy {
  implicit val canGroupByVector: CanGroupBy[Vector] = new CanGroupBy[Vector] {
    def groupBy[K: ClassTag, V: ClassTag](fa: Vector[V])(f: V => K): Vector[(K, Iterable[V])] =
      fa.groupBy(f).toVector

    override def distinct[A: ClassTag](fa: Vector[A]): Vector[A]                  = fa.distinct
    def distinctBy[A: ClassTag, B: ClassTag](fa: Vector[A])(f: A => B): Vector[A] = fa.groupBy(f).map(_._2.head).toVector
  }

  implicit val canGroupByParVector: CanGroupBy[ParVector] = new CanGroupBy[ParVector] {
    def groupBy[K: ClassTag, V: ClassTag](fa: ParVector[V])(f: V => K): ParVector[(K, Iterable[V])] = {
      val res = fa.groupBy(f).mapValues(_.seq).toVector
      ParVector(res: _*)
    }

    override def distinct[A: ClassTag](fa: ParVector[A]): ParVector[A] = fa.distinct
    def distinctBy[A: ClassTag, B: ClassTag](
        fa: ParVector[A]
    )(f: A => B): ParVector[A] = {
      val res = fa.groupBy(f).map(_._2.head).toVector
      ParVector(res: _*)
    }
  }
}
