package com.github.trembita.spark

import org.apache.spark.rdd.RDD
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

class RunFutureOnSpark(timeout: AsyncTimeout) extends RunOnSpark[Future] {
  def traverse[A, B: ClassTag](rdd: RDD[A])(f: A => Future[B]): RDD[B] =
    rdd.mapPartitions { partition =>
      val _f      = f
      val mapped  = partition.map(_f)
      val awaited = mapped.map(Await.result(_, timeout.duration))
      awaited
    }

  def lift[A](rdd: RDD[A]): Future[RDD[A]] = SerializableFuture.pure(rdd)
}
