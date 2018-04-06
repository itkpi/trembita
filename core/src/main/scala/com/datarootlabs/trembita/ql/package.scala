package com.datarootlabs.trembita


import language.{existentials, higherKinds}
import language.experimental.macros
import scala.reflect.macros.blackbox
import scala.annotation.{implicitNotFound, tailrec}
import scala.concurrent.{Await, ExecutionContext, Future}
import cats._
import cats.implicits._
import utils._
import shapeless._
import shapeless.ops.hlist.At
import algebra.ring._
import ql.instances._
import ql.AggRes._
import ql.AggDecl._
import ql.QueryBuilder._
import ql.GroupingCriteria._
import ql.ArbitraryGroupResult._
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._


package object ql extends aggregationInstances {
  type GroupedLazyListWithTotals[A, K, T <: HList, LzyImpl[_] <: DataPipeline[_]] =
    LzyImpl[GroupWithTotalResult[K, T, A]]

  type Grouped2LazyListWithTotals[A, K1, K2, T <: HList, LzyImpl[_] <: DataPipeline[_]] =
    LzyImpl[GroupWithTotal2Result[K1, K2, T, A]]

  implicit class FunctionalOps[A](val self: DataPipeline[A]) extends AnyVal {


    def groupByWithTotal[K, T <: HList : Monoid](extract: A => T)
                                                (groupFunc: A => K): GroupedLazyListWithTotals[A, K, T, DataPipeline] = {
      val monoid = Monoid[T]
      new StrictSource[GroupWithTotalResult[K, T, A]]({
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
        map.map { case (key, (totals, records)) => GroupWithTotalResult(key, totals, records) }
      })
    }

    def groupByWithTotal2[K1, K2, T <: HList : Monoid](extract: A => T)
                                                      (groupFunc1: A => K1,
                                                       groupFunc2: A => K2): Grouped2LazyListWithTotals[A, K1, K2, T, DataPipeline] = {
      new StrictSource[GroupWithTotal2Result[K1, K2, T, A]]({
        val forced: Iterable[A] = self.force
        var mapByBothKeys = Map.empty[(K1, K2), (T, Iterable[A])]
        for (item <- forced) {
          val key1 = groupFunc1(item)
          val key2 = groupFunc2(item)
          val bothKeys = (key1, key2)
          if (mapByBothKeys contains bothKeys) {
            val (acc, currentGroup) = mapByBothKeys(bothKeys)
            mapByBothKeys = mapByBothKeys.updated(bothKeys,
              (acc |+| extract(item), currentGroup ++: Some(item)))
          } else {
            mapByBothKeys += (bothKeys -> (extract(item), Vector(item)))
          }
        }
        var mapWithKey1Totals = Map.empty[K1, (T, Iterable[GroupWithTotalResult[K2, T, A]])]
        for (((key1, key2), (key2Totals, key2Records)) <- mapByBothKeys) {
          if (mapWithKey1Totals contains key1) {
            val (acc, key2GroupsWithTotals) = mapWithKey1Totals(key1)
            mapWithKey1Totals = mapWithKey1Totals.updated(key1,
              (acc |+| key2Totals, key2GroupsWithTotals ++: key2GroupsWithTotals))
          } else {
            mapWithKey1Totals += (key1 ->
              (key2Totals, Vector(GroupWithTotalResult(key2, key2Totals, key2Records))))
          }
        }
        mapWithKey1Totals.map { case (key1, (key1Totals, key2Records)) =>
          GroupWithTotal2Result(key1, key1Totals, key2Records)
        }
      })
    }

    def groupByWithTotalPar[K, T <: HList : Monoid](extract: A => T)
                                                   (groupFunc: A => K)
                                                   (implicit ec: ExecutionContext): GroupedLazyListWithTotals[A, K, T, ParDataPipeline] = {
      val monoid = Monoid[T]
      new ParSource[GroupWithTotalResult[K, T, A]]({
        val forced: Iterable[A] = self.force
        val batches: Iterable[Iterable[A]] = ListUtils.split(ParDataPipeline.defaultParallelism)(forced)
        val future: Future[Iterable[Map[K, GroupWithTotalResult[K, T, A]]]] = Future.sequence {
          batches.map { batch =>
            Future {
              var map = Map.empty[K, GroupWithTotalResult[K, T, A]]
              for (item <- batch) {
                val key = groupFunc(item)
                if (map contains key) {
                  val acc = map(key)
                  map = map.updated(key, acc ++
                    GroupWithTotalResult(key, extract(item), Vector(item)))
                } else {
                  map += (key -> GroupWithTotalResult(key, extract(item), Vector(item)))
                }
              }
              map
            }
          }
        }
        Await.result(future, ParDataPipeline.defaultTimeout).reduce { (leftMap, rightMap) =>
          leftMap.mergeConcat(rightMap)(_ ++ _)
        }.values
      })
    }

    def groupByWithTotal2Par[K1, K2, T <: HList : Monoid](extract: A => T)
                                                         (groupFunc1: A => K1,
                                                          groupFunc2: A => K2)
                                                         (implicit ec: ExecutionContext): Grouped2LazyListWithTotals[A, K1, K2, T, ParDataPipeline] = {
      new ParSource[GroupWithTotal2Result[K1, K2, T, A]]({
        val forced: Iterable[A] = self.force
        val batches: Iterable[Iterable[A]] = ListUtils.split(ParDataPipeline.defaultParallelism)(forced)
        val futureByBothKeys: Future[Iterable[Map[(K1, K2), GroupWithTotalResult[K2, T, A]]]] = Future.sequence {
          batches.map { batch =>
            Future {
              var mapByBothKeys = Map.empty[(K1, K2), GroupWithTotalResult[K2, T, A]]
              for (item <- batch) {
                val key1 = groupFunc1(item)
                val key2 = groupFunc2(item)
                val bothKeys = (key1, key2)
                if (mapByBothKeys contains bothKeys) {
                  val acc = mapByBothKeys(bothKeys)
                  mapByBothKeys = mapByBothKeys.updated(bothKeys,
                    acc ++ GroupWithTotalResult(key2, extract(item), Vector(item)))
                } else {
                  mapByBothKeys += (bothKeys -> GroupWithTotalResult(key2, extract(item), Vector(item)))
                }
              }
              mapByBothKeys
            }
          }
        }
        /** -- */
        val futureByKey1: Future[Map[K1, GroupWithTotal2Result[K1, K2, T, A]]] = futureByBothKeys.map { processedBatches =>
          val merged: Map[(K1, K2), GroupWithTotalResult[K2, T, A]] = processedBatches
            .reduce { (leftMap, rightMap) => leftMap.mergeConcat(rightMap)(_ ++ _) }
          var mapWithKey1Totals = Map.empty[K1, GroupWithTotal2Result[K1, K2, T, A]]
          for (((key1, _), key2Totals) <- merged) {
            if (mapWithKey1Totals contains key1) {
              val acc = mapWithKey1Totals(key1)
              mapWithKey1Totals = mapWithKey1Totals.updated(key1,
                acc :+ key2Totals)
            } else {
              mapWithKey1Totals += (key1 -> GroupWithTotal2Result(key1, key2Totals.totals, Vector(key2Totals)))
            }
          }
          mapWithKey1Totals
        }
        Await.result(futureByKey1, ParDataPipeline.defaultTimeout).values
      })
    }


    def collectWithTotal[T <: HList : Monoid](extract: A => T): (T, Iterable[A]) = {
      val monoid = Monoid[T]
      val (totals, builder) = self.foldLeft((monoid.empty, Vector.newBuilder[A])) {
        case ((acc, xbuilder), item) => (acc |+| extract(item), xbuilder += item)
      }
      (totals, builder.result())
    }

    def split(parts: Int): DataPipeline[Iterable[A]] =
      new StrictSource[Iterable[A]](ListUtils.split(parts)(self.force))

    def reduceMonoid(implicit monoid: Monoid[A]): A = self.foldLeft(monoid.empty)(monoid.combine)

    def collectWithTotalPar[T <: HList : Monoid](extract: A => T)(implicit ec: ExecutionContext): (T, Iterable[A]) = {
      val monoid = Monoid[T]
      val cached = self.cache()
      val groups = new StrictSource[Iterable[A]](ListUtils.split(ParDataPipeline.defaultParallelism)(cached.force))
      val t = groups.par
        .map(_.foldLeft(monoid.empty)((acc, a) => acc |+| extract(a)))
        .reduceMonoid
      (t, cached.force)
    }
  }

  implicit class TaggingSyntax[A](val self: A) extends AnyVal {
    def as[T]: A ## T = new ##[A, T](self)
  }


  implicit class GroupingCriteriaOps[G <: GroupingCriteria](val self: G) extends AnyVal {
    def &::[GH <: ##[_, _]](head: GH): GH &:: G = GroupingCriteria.&::(head, self)
    def apply(n: Nat)(implicit at: GroupingCriteria.At[G, n.N]): at.Out = at(self)
  }

  implicit class AggregationNameOps[A <: AggDecl](val self: A) {
    def %::[GH <: TaggedAgg[_, _, _]](head: GH): GH %:: A = AggDecl.%::(head, self)
  }

  implicit class AggResOps[A <: AggRes](val self: A) {
    def *::[H <: ##[_, _]](head: H): H *:: A = AggRes.*::(head, self)
    def apply[U](implicit get: AggRes.Get[A, U]): get.Out = get(self)
    def get[U](implicit gget: AggRes.Get[A, U]): gget.Out = gget(self)
  }


  implicit class TaggingOps[A, U](val self: A ## U) extends AnyVal {
    def sum: TaggedAgg[A, U, AggFunc.Type.Sum] = TaggedAgg(self)
    def avg: TaggedAgg[A, U, AggFunc.Type.Avg] = TaggedAgg(self)
    def count: TaggedAgg[A, U, AggFunc.Type.Count] = TaggedAgg(self)
  }

  implicit class TrembitaQL[A](val self: Seq[A]) extends AnyVal {
    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes]
    (queryF: Empty[A] ⇒ Query[A, G, T, R])
    (implicit trembitaql: trembitaql[A, G, T, R]): ArbitraryGroupResult[A, G, AggFunc.Result[T, R, Query[A, G, T, R]#Comb]] =
      trembitaql(self, queryF)
  }

  implicit class TrembitaQLForPipeline[A](val self: DataPipeline[A]) extends AnyVal {
    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes]
    (queryF: Empty[A] ⇒ Query[A, G, T, R])(implicit trembitaql: trembitaql[A, G, T, R])
    : DataPipeline[ArbitraryGroupResult[A, G, AggFunc.Result[T, R, Query[A, G, T, R]#Comb]]] = DataPipeline.from({
      val forced = self.force
      val result = trembitaql(forced.toSeq, queryF)
      Seq(result)
    })
  }

  implicit def hListMonoid[K <: HList]: Monoid[K] = macro hListMonoidImpl[K]
  implicit def arbitraryGroupResultMonoid[A, K <: GroupingCriteria, T]: Monoid[ArbitraryGroupResult[A, K, T]] = macro arbitraryGroupResultMonoidImpl[A, K, T]
}