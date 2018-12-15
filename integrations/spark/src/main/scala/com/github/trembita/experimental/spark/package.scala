package com.github.trembita.experimental

import scala.language.experimental.macros
import scala.language.implicitConversions
import com.github.trembita.{InjectTaggedK, MagnetM}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

package object spark {
  implicit val runIdOnSpark: RunOnSpark[cats.Id] = new RunIdOnSpark

  implicit def runFutureOnSpark(implicit timeout: Timeout): RunOnSpark[Future] =
    new RunFutureOnSpark(timeout)

  implicit def deriveSpark(implicit sc: SparkContext): Spark = Spark.derive(sc)

  implicit def materializeFuture[A, B](
    f: A => Future[B]
  ): MagnetM[Future, A, B, Spark] = macro rewrite.materializeFutureImpl[A, B]

  implicit def turnVectorIntoRDD(
    implicit sc: SparkContext
  ): InjectTaggedK[Vector, RDD] = new InjectTaggedK[Vector, RDD] {
    def apply[A: ClassTag](fa: Vector[A]): RDD[A] = sc.parallelize(fa)
  }

  implicit def turnRDDIntoVector(
    implicit sc: SparkContext
  ): InjectTaggedK[RDD, Vector] = new InjectTaggedK[RDD, Vector] {
    def apply[A: ClassTag](fa: RDD[A]): Vector[A] = fa.collect().toVector
  }
}
