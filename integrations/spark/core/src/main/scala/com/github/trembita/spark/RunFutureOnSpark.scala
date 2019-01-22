package com.github.trembita.spark

import org.apache.spark.rdd.RDD
import scala.concurrent.Await
import scala.reflect.ClassTag

class RunFutureOnSpark(timeout: AsyncTimeout) extends RunOnSpark[SerializableFuture] {
  def traverse[A, B: ClassTag](rdd: RDD[A])(f: A => SerializableFuture[B]): RDD[B] =
    rdd.mapPartitions { partition =>
      val mapped  = partition.map(f)
      val awaited = mapped.map(Await.result(_, timeout.duration))
      awaited
    }

  def lift[A](rdd: RDD[A]): SerializableFuture[RDD[A]] = SerializableFuture.pure(rdd)
}
