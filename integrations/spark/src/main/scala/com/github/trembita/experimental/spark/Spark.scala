package com.github.trembita.experimental.spark

import com.github.trembita._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import scala.language.higherKinds
import scala.reflect.ClassTag

sealed trait Spark extends Execution {
  final type Repr[X] = RDD[X]
  type Run[G[_]] = RunOnSpark[G]

  def toVector[A](repr: RDD[A]): Vector[A] = repr.collect().toVector

  def groupBy[A, K: ClassTag](vs: Repr[A])(f: A => K): Repr[(K, Iterable[A])] =
    vs.groupBy(f)
  def collect[A, B: ClassTag](
    repr: Repr[A]
  )(pf: PartialFunction[A, B]): Repr[B] =
    repr.collect(pf)
  def sorted[A: Ordering: ClassTag](repr: Repr[A]): Repr[A] =
    repr.sortBy(identity)
  def concat[A](xs: Repr[A], ys: Repr[A]): Repr[A] = xs union ys
  def zip[A, B: ClassTag](xs: Repr[A], ys: Repr[B]): Repr[(A, B)] = xs zip ys

  def distinctKeys[A: ClassTag, B: ClassTag](repr: RDD[(A, B)]): RDD[(A, B)] =
    repr.reduceByKey((a, b) => a)

  def memoize[A: ClassTag](xs: RDD[A]): RDD[A] = xs.persist()

  val ApplicativeFlatMap: ApplicativeFlatMap[RDD] =
    new ApplicativeFlatMap[RDD] {
      def flatMap[A, B: ClassTag](fa: RDD[A])(f: A => RDD[B]): RDD[B] =
        fa.flatMap(f(_).collect())

      def map[A, B: ClassTag](fa: RDD[A])(f: A => B): RDD[B] = fa.map(f)
    }

  val Traverse: TraverseTag[RDD, RunOnSpark] =
    new TraverseTag[RDD, RunOnSpark] {
      def traverse[G[_], A, B: ClassTag](
        fa: RDD[A]
      )(f: A => G[B])(implicit G: RunOnSpark[G]): G[RDD[B]] = G.lift {
        G.traverse(fa)(f)
      }
    }
}

object Spark {
  implicit val spark: Spark = new Spark {}
}
