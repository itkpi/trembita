package trembita.operations

import scala.collection.parallel.immutable.ParVector
import scala.language.higherKinds
import scala.reflect.ClassTag
import trembita.collections._
import trembita.internal.ListUtils

import scala.collection.{immutable, mutable}

trait CanCombineByKey[F[_]] {
  def combineByKey[K: ClassTag, V: ClassTag, C: ClassTag](fa: F[(K, V)])(
      init: V => C,
      addValue: (C, V) => C,
      mergeCombiners: (C, C) => C
  ): F[(K, C)]
}

trait CanCombineByKeyWithParallelism[F[_]] extends CanCombineByKey[F] {
  def combineByKey[K: ClassTag, V: ClassTag, C: ClassTag](fa: F[(K, V)], parallelism: Int)(
      init: V => C,
      addValue: (C, V) => C,
      mergeCombiners: (C, C) => C
  ): F[(K, C)]

  override def combineByKey[K: ClassTag, V: ClassTag, C: ClassTag](
      fa: F[(K, V)]
  )(init: V => C, addValue: (C, V) => C, mergeCombiners: (C, C) => C): F[(K, C)] =
    combineByKey(fa, parallelism = Runtime.getRuntime.availableProcessors())(init, addValue, mergeCombiners)
}

object CanCombineByKeyWithParallelism {
  implicit val canCombineParVectorByKey: CanCombineByKeyWithParallelism[ParVector] = new CanCombineByKeyWithParallelism[ParVector] {
    def combineByKey[K: ClassTag, V: ClassTag, C: ClassTag](
        fa: ParVector[(K, V)],
        parallelism: Int
    )(init: V => C, addValue: (C, V) => C, mergeCombiners: (C, C) => C): ParVector[(K, C)] = {
      val batched = ListUtils.batch(parts = parallelism)(fa.seq)
      val processed = batched.par
        .map { batch =>
          val iterator             = batch.iterator
          val (initKey, initValue) = iterator.next()
          var prevKey              = initKey
          var currComb             = init(initValue)
          val map                  = mutable.OpenHashMap.empty[K, C]
          while (iterator.hasNext) {
            val (currKey, currValue) = iterator.next()
            if (prevKey == currKey) {
              currComb = addValue(currComb, currValue)
            } else {
              if (!map.contains(prevKey)) {
                map += (prevKey -> currComb)
              } else {
                map.update(prevKey, currComb)
              }
              currComb = if (map contains currKey) {
                addValue(map(currKey), currValue)
              } else {
                init(currValue)
              }
              prevKey = currKey
            }
          }
          if (map contains prevKey) map.update(prevKey, currComb)
          else map += (prevKey -> currComb)
          map.toMap
        }

      processed.reduce(_.mergeConcat(_)(mergeCombiners).toMap).seq.toVector.par
    }
  }
}

object CanCombineByKey {
  implicit val canCombineVectorByKey: CanCombineByKey[Vector] = new CanCombineByKey[Vector] {
    def combineByKey[K: ClassTag, V: ClassTag, C: ClassTag](
        fa: Vector[(K, V)]
    )(init: V => C, addValue: (C, V) => C, mergeCombiners: (C, C) => C): Vector[(K, C)] =
      fa.aggregate(Map.empty[K, C])(
          seqop = {
            case (acc, (k, v)) =>
              if (acc contains k) acc.updated(k, addValue(acc(k), v))
              else acc + (k -> init(v))
          },
          combop = { (acc1, acc2) =>
            acc1.mergeConcat(acc2)(mergeCombiners).seq.toMap
          }
        )
        .toVector
  }

  implicit val canCombineParVectorByKey: CanCombineByKey[ParVector] = CanCombineByKeyWithParallelism.canCombineParVectorByKey
}
