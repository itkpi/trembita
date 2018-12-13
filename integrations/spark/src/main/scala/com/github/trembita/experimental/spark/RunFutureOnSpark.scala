package com.github.trembita.experimental.spark

import org.apache.spark.rdd.RDD

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

case class Timeout(duration: FiniteDuration)

class RunFutureOnSpark(timeout: Timeout) extends RunOnSpark[Future] {
  def traverse[A, B: ClassTag](rdd: RDD[A])(f: A => Future[B]): RDD[B] =
    rdd.mapPartitions { partition =>
      val mapped = partition.map(f)
      val awaited = mapped.map(Await.result(_, timeout.duration))
      awaited
    }


  def lift[A](rdd: RDD[A]): Future[RDD[A]] = Future.successful(rdd)
}
