package com.datarootlabs.trembita.parallel


import com.datarootlabs.trembita._
import com.datarootlabs.trembita.internal._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag


trait ParDataPipeline[+A] extends DataPipeline[A] {
  protected implicit def ec: ExecutionContext
  override final def par(implicit ec2: ExecutionContext): ParDataPipeline[A] = this
  override def sorted[B >: A : Ordering : ClassTag]: DataPipeline[B] = new ParSortedSource[B](this)

  def sum[B >: A](implicit num: Numeric[B]): B = combine(num.zero, num.plus, num.plus)
  def combine[C](init: => C,
                 add: (C, A) => C,
                 merge: (C, C) => C): C = {
    val res: C = init
    val forced: Iterable[A] = this.force
    val combiners: Iterable[Future[C]] = ListUtils.split(ParDataPipeline.defaultParallelism)(forced)
      .map { group =>
        val futureRes = Future {
          group.foldLeft(init)(add)
        }
        futureRes
      }
    val futureRes: Future[Iterable[C]] = Future.sequence(combiners)
    val result: C = Await.result(futureRes, ParDataPipeline.defaultTimeout).foldLeft(res)(merge)
    result
  }
}

object ParDataPipeline {
  val defaultTimeout    : FiniteDuration = 5.minutes
  val defaultParallelism: Int            = Runtime.getRuntime.availableProcessors()
}
