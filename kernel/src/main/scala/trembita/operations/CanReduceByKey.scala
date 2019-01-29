package trembita.operations

import scala.collection.concurrent.TrieMap
import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag

trait CanReduceByKey[F[_]] {
  def reduceByKey[K: ClassTag, V: ClassTag](fa: F[(K, V)])(reduce: (V, V) => V): F[(K, V)]
}

object CanReduceByKey {
  implicit val canReduceVectorByKey: CanReduceByKey[Vector] = new CanReduceByKey[Vector] {
    def reduceByKey[K: ClassTag, V: ClassTag](fa: Vector[(K, V)])(
        reduce: (V, V) => V
    ): Vector[(K, V)] = {
      var map = Map.empty[K, V]
      for ((k, v) <- fa) {
        if (map contains k) map = map.updated(k, reduce(map(k), v))
        else map += (k -> v)
      }
      map.toVector
    }
  }

  implicit val canReduceParVectorByKey: CanReduceByKey[ParVector] = new CanReduceByKey[ParVector] {
    def reduceByKey[K: ClassTag, V: ClassTag](fa: ParVector[(K, V)])(
        reduce: (V, V) => V
    ): ParVector[(K, V)] = {
      val map = TrieMap.empty[K, V]
      for ((k, v) <- fa) {
        if (map contains k) map.update(k, reduce(map(k), v))
        else map += (k -> v)
      }
      map.toVector.par
    }
  }
}
