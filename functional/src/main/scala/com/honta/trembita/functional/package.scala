package com.honta.trembita


import cats.Monoid
import cats.implicits._
import shapeless._
import com.honta.trembita.internal.{ListUtils, StrictSource}
import com.honta.trembita.parallel._
import com.honta.trembita.utils.IterableExtended

import scala.concurrent.{Await, ExecutionContext, Future}


package object functional extends AllInstances {
  implicit class FunctionalOps[A](val self: LazyList[A]) extends AnyVal {
    def groupByWithTotal[K, T <: HList : Monoid](extract: A => T)(groupFunc: A => K): LazyList[(K, (T, Iterable[A]))] = {
      val monoid = Monoid[T]
      new StrictSource[(K, (T, Iterable[A]))]({
        val forced: Iterable[A] = self.force
        var map = Map.empty[K, (T, Iterable[A])]
        for (item <- forced) {
          val key = groupFunc(item)
          if (map contains key) {
            val (acc, currentGroup) = map(key)
            map = map.updated(key, (monoid.combine(acc, extract(item)), currentGroup ++: Some(item)))
          } else {
            map += (key -> (extract(item), Vector(item)))
          }
        }
        map
      })
    }

    def groupByWithTotalPar[K, T <: HList : Monoid](extract: A => T)
                                                   (groupFunc: A => K)
                                                   (implicit ec: ExecutionContext): ParLazyList[(K, (T, Iterable[A]))] = {
      val monoid = Monoid[T]
      new ParSource[(K, (T, Iterable[A]))]({
        val forced: Iterable[A] = self.force
        val batches: Iterable[Iterable[A]] = ListUtils.split(ParLazyList.defaultParallelism)(forced)
        val future: Future[Iterable[Map[K, (T, Iterable[A])]]] = Future.sequence {
          batches.map { batch =>
            Future {
              var map = Map.empty[K, (T, Iterable[A])]
              for (item <- batch) {
                val key = groupFunc(item)
                if (map contains key) {
                  val (acc, currentGroup) = map(key)
                  map = map.updated(key, (monoid.combine(acc, extract(item)), currentGroup ++: Some(item)))
                } else {
                  map += (key -> (extract(item), Vector(item)))
                }
              }
              map
            }
          }
        }
        Await.result(future, ParLazyList.defaultTimeout).reduce { (leftMap, rightMap) =>
          leftMap.mergeConcat(rightMap) {
            case ((hlist1, iterables1), (hlist2, iterables2)) =>
              (hlist1 |+| hlist2, iterables1 ++ iterables2)
          }
        }
      })
    }

    def collectWithTotal[T <: HList : Monoid](extract: A => T): (T, Iterable[A]) = {
      val monoid = Monoid[T]
      val (totals, builder) = self.foldLeft((monoid.empty, Vector.newBuilder[A])) {
        case ((acc, xbuilder), item) => (acc |+| extract(item), xbuilder += item)
      }
      (totals, builder.result())
    }

    def split(parts: Int): LazyList[Iterable[A]] =
      new StrictSource[Iterable[A]](ListUtils.split(parts)(self.force))

    def reduceMonoid(implicit monoid: Monoid[A]): A = self.foldLeft(monoid.empty)(monoid.combine)

    def collectWithTotalPar[T <: HList : Monoid](extract: A => T)(implicit ec: ExecutionContext): (T, Iterable[A]) = {
      val monoid = Monoid[T]
      val cached = self.cache()
      val groups = new StrictSource[Iterable[A]](ListUtils.split(ParLazyList.defaultParallelism)(cached.force))
      val t = groups.par
        .map(_.foldLeft(monoid.empty)((acc, a) => acc |+| extract(a)))
        .reduceMonoid
      (t, cached.force)
    }
  }
}
