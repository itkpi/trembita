package com.github.trembita.operations
import scala.annotation.implicitNotFound
import scala.collection.parallel.immutable.ParVector
import scala.collection.mutable
import scala.language.higherKinds
import scala.reflect.ClassTag

@implicitNotFound("""
    ${F} does not support spanBy operation or it is not efficient.
    If you want to spanBy ${F}, please provide an implicit instance in scope
  """)
trait CanSpanBy[F[_]] {
  def spanBy[K: ClassTag, V: ClassTag](fa: F[V])(f: V => K): F[(K, Iterable[V])]
}

object CanSpanBy {
  implicit val canSpanByVector: CanSpanBy[Vector] = new CanSpanBy[Vector] {
    def spanBy[K: ClassTag, V: ClassTag](fa: Vector[V])(f: V => K): Vector[(K, Iterable[V])] = {
      val buffers            = mutable.Map.empty[K, mutable.ListBuffer[V]]
      var currKey: Option[K] = None
      for (v <- fa) {
        val key = f(v)
        currKey match {
          case Some(`key`) =>
            buffers.update(key, buffers(key) :+ v)

          case _ =>
            buffers += (key -> mutable.ListBuffer(v))
            currKey = Some(key)
        }
      }
      buffers.mapValues(_.toVector).toVector
    }
  }

  implicit val canSpanByParVector: CanSpanBy[ParVector] = new CanSpanBy[ParVector] {
    def spanBy[K: ClassTag, V: ClassTag](fa: ParVector[V])(f: V => K): ParVector[(K, Iterable[V])] = {
      val buffers            = mutable.Map.empty[K, mutable.ListBuffer[V]]
      var currKey: Option[K] = None
      for (v <- fa) {
        val key = f(v)
        currKey match {
          case Some(`key`) =>
            buffers.update(key, buffers(key) :+ v)

          case _ =>
            buffers += (key -> mutable.ListBuffer(v))
            currKey = Some(key)
        }
      }
      buffers.mapValues(_.toVector).toVector.par
    }
  }

}
