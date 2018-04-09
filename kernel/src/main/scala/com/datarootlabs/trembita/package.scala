package com.datarootlabs


import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.effect.Sync
import com.datarootlabs.trembita.parallel._
import com.datarootlabs.trembita.internal._

import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag


package object trembita {
  type PairPipeline[K, V] = DataPipeline[(K, V)]
  type ParPairPipeline[K, V] = ParDataPipeline[(K, V)]

  /**
    * Operations for [[DataPipeline]] of tuples
    * (NOT [[MapPipeline]])
    **/
  implicit class PairPipelineOps[K, V](val self: PairPipeline[K, V]) extends AnyVal {
    def mapValues[W](f: V ⇒ W): PairPipeline[K, W] = self.map { case (k, v) ⇒ (k, f(v)) }
    def keys: DataPipeline[K] = self.map(_._1)
    def values: DataPipeline[V] = self.map(_._2)

    /**
      * Merges all values [[V]]
      * with the same key [[K]]
      *
      * @param f - reducing function
      * @return - reduced pipeline
      **/
    def reduceByKey(f: (V, V) ⇒ V): PairPipeline[K, V] =
      self.groupBy(_._1).mapValues { vs ⇒
        vs.foldLeft(Option.empty[V]) {
          case (None, (_, v)) ⇒ Some(v)
          case (acc, (_, v))  ⇒ acc.map(f(_, v))
        }.get
      }

    /**
      * Same to [[reduceByKey]] above
      * Merges all values [[V]]
      * having a [[Monoid]] defined
      *
      * @param vMonoid - Monoid for [[V]]
      * @return - reduced pipeline
      **/
    def reduceByKey(implicit vMonoid: Monoid[V]): PairPipeline[K, V] = reduceByKey((v1, v2) ⇒ vMonoid.combine(v1, v2))

    /** @return - [[MapPipeline]] */
    def toMap: MapPipeline[K, V] = new BaseMapPipeline[K, V](self)
  }

  /**
    * Implementation of [[Monad]] for [[DataPipeline]]
    **/
  implicit object DataPipelineMonad extends Monad[DataPipeline] {
    override def pure[A](x: A): DataPipeline[A] = DataPipeline(x)
    override def flatMap[A, B](fa: DataPipeline[A])(f: A ⇒ DataPipeline[B]): DataPipeline[B] = fa.flatMap(f(_).eval)
    override def tailRecM[A, B](a: A)(f: A ⇒ DataPipeline[Either[A, B]]): DataPipeline[B] = f(a).flatMap {
      case Left(xa) ⇒ tailRecM(xa)(f).eval
      case Right(b) ⇒ Some(b)
    }
  }

  implicit class Ops[A](val self: DataPipeline[A]) extends AnyVal {
    /**
      * Provides better syntax for [[DataPipeline.runM]]
      * Used to avoid writing something like
      * {{{
      *   pipeline.run[Int, IO]
      *   // or
      *   pipeline.run(Sync[IO])
      * }}}
      * due to variance
      **/
    def run[M[_] : Sync]: M[Iterable[A]] = self.runM(Sync[M])


    /**
      * Orders elements of the [[DataPipeline]]
      * by the result of function application
      * having an [[Ordering]] defined for type [[BB]]
      *
      * @tparam BB - sorting criteria
      * @param f - function to extract a [[BB]] instance from pipeline element
      * @return - the same pipeline sorted by [[BB]]
      **/
    def sortBy[BB: Ordering](f: A ⇒ BB)(implicit ctg: ClassTag[A]): DataPipeline[A] =
      self.sorted(Ordering.by[A, BB](f), ctg)

    /**
      * Evaluates the pipeline
      * as an arbitrary collection
      *
      * @tparam Coll - type of the resulting collection
      * @param cbf - [[CanBuildFrom]] instance for the collection
      * @return - pipeline's values wrapped into [[Coll]]
      **/
    def evalAs[Coll[_]](implicit cbf: CanBuildFrom[Coll[A], A, Coll[A]]): Coll[A] = self.evalAsColl[A, Coll]
  }

  implicit def iterable2DataPipeline[A](iter: Iterable[A]): DataPipeline[A] = DataPipeline.from(iter)
  implicit def option2DataPipeline[A](opt: Option[A]): DataPipeline[A] = DataPipeline.from(opt)
}
