package trembita
import cats.{Monad, Monoid}
import cats.implicits._

import scala.collection.{GenIterable, GenMap, GenTraversable, GenTraversableOnce}
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.SortedMap
import scala.collection.parallel.ParMap
import scala.language.higherKinds

package object collections {
  implicit class IterableExtended[A](val self: GenIterable[A]) extends AnyVal {
    def minMax(implicit cmp: Ordering[A]): (A, A) = {
      if (self.isEmpty)
        throw new UnsupportedOperationException("empty.minMax")

      self.foldLeft((self.head, self.head)) {
        case ((min, max), elem) =>
          val newMin = if (cmp.gt(min, elem)) elem else min
          val newMax = if (cmp.lt(max, elem)) elem else max
          (newMin, newMax)
      }
    }

    def minMaxBy[U](f: A => U)(implicit cmp: Ordering[U]): (A, A) = {
      if (self.isEmpty)
        throw new UnsupportedOperationException("empty.minMaxBy")

      self.foldLeft((self.head, self.head)) {
        case ((min, max), elem) =>
          val felem  = f(elem)
          val fmin   = f(min)
          val fmax   = f(max)
          val newMin = if (cmp.gt(fmin, felem)) elem else min
          val newMax = if (cmp.lt(fmax, felem)) elem else max
          (newMin, newMax)
      }
    }

    def minMaxByOpt[U](f: A => U)(implicit cmp: Ordering[U]): Option[(A, A)] =
      if (self.isEmpty) None
      else Some(self.minMaxBy(f))

    def mergeConcat[K, V, Col[_]](
        rhs: GenTraversable[(K, V)]
    )(concatOp: (V, V) => V)(implicit ev: A <:< (K, V), cbf: CanBuildFrom[Nothing, (K, V), Col[(K, V)]]): Col[(K, V)] = {
      val lfs = self.toMap[K, V]
      lfs
        .foldLeft(rhs.map { case (k, v) => k -> (v :: Nil) }.toMap) {
          case (acc, (k, v)) if acc contains k => acc.updated(k, v :: acc(k))
          case (acc, (k, v))                   => acc + (k -> (v :: Nil))
        }
        .map { case (k, vs) => k -> vs.reduce[V](concatOp) }
        .to[Col]
    }

    def merge[K, V: Monoid](
        rhs: Map[K, V]
    )(implicit ev: A <:< (K, V)): Map[K, V] = {
      val lfs = self.toMap[K, V]
      lfs
        .foldLeft(rhs.map { case (k, v) => k -> (v :: Nil) }) {
          case (acc, (k, v)) if acc contains k => acc.updated(k, v :: acc(k))
          case (acc, (k, v))                   => acc + (k -> (v :: Nil))
        }
        .mapValues(_.reduce[V](_ |+| _))
    }

    def minOptBy[U: Ordering](f: A => U): Option[A] =
      if (self.isEmpty) None
      else Some(self.min(Ordering.by(f)))

    def maxOptBy[U: Ordering](f: A => U): Option[A] =
      if (self.isEmpty) None
      else Some(self.max(Ordering.by(f)))
  }

  implicit class MapOps[K, V](map: Map[K, V]) {
    def modify(key: K, default: => V)(f: V => V): Map[K, V] =
      if (map contains key) {
        map.updated(key, f(map(key)))
      } else map + (key -> default)
  }

  implicit class SortedMapOps[K, V](map: SortedMap[K, V]) {
    def modify(key: K, default: => V)(f: V => V): SortedMap[K, V] =
      if (map contains key) {
        map.updated(key, f(map(key)))
      } else map + (key -> default)
  }
}
