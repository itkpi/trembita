package com.honta


import com.honta.trembita.internal.{BaseLazyMap, LazyMap}
import com.honta.trembita.parallel.ParLazyList

package object trembita {
  type PairLazyList[K, V] = LazyList[(K, V)]
  type ParPairLazyList[K, V] = ParLazyList[(K, V)]

  implicit class PairLazyListOps[K, V](val self: PairLazyList[K, V]) extends AnyVal {
    def mapValues[W](f: V => W): PairLazyList[K, W] = self.map { case (k, v) => (k, f(v)) }
    def reduceByKey(f: (V, V) => V): PairLazyList[K, V] =
      self.groupBy(_._1).mapValues { vs =>
        vs.foldLeft(Option.empty[V]) {
          case (None, (_, v)) => Some(v)
          case (acc, (_, v))  => acc.map(f(_, v))
        }.get
      }
    def toMap: LazyMap[K, V] = new BaseLazyMap[K, V](self)
  }
}
