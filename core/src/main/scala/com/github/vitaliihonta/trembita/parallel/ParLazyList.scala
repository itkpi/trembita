package com.github.vitaliihonta.trembita.parallel

import com.github.vitaliihonta.trembita.LazyList
import com.github.vitaliihonta.trembita.internal._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag


trait ParLazyList[+A] extends LazyList[A] {
  protected implicit def ec: ExecutionContext
  override final def par(implicit ec2: ExecutionContext): ParLazyList[A] = this
  override def sorted[B >: A : Ordering : ClassTag]: LazyList[B] = new ParSortedSource[B](this)

  def sum[B >: A](implicit num: Numeric[B]): B = combine(num.zero, num.plus, num.plus)
  def combine[C](init: => C,
                 add: (C, A) => C,
                 merge: (C, C) => C): C = {
    val res: C = init
    val forced: Iterable[A] = this.force
    val combiners: Iterable[Future[C]] = ListUtils.split(ParLazyList.defaultParallelism)(forced)
      .map { group =>
        val futureRes = Future {
          group.foldLeft(init)(add)
        }
        futureRes
      }
    val futureRes: Future[Iterable[C]] = Future.sequence(combiners)
    val result: C = Await.result(futureRes, ParLazyList.defaultTimeout).foldLeft(res)(merge)
    result
  }
}

object ParLazyList {
  val defaultTimeout: FiniteDuration = 5.minutes
  val defaultParallelism: Int = Runtime.getRuntime.availableProcessors()
}
