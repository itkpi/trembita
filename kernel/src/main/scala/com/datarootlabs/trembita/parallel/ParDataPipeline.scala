package com.datarootlabs.trembita.parallel


import cats.Monoid
import com.datarootlabs.trembita._
import com.datarootlabs.trembita.internal._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag


/**
  * Parallel implementation of [[DataPipeline]]
  **/
trait ParDataPipeline[+A] extends DataPipeline[A] {
  /** Execution context */
  protected implicit def ec: ExecutionContext

  /** [[ParDataPipeline.par]] = this */
  final def par(implicit ec2: ExecutionContext): this.type = this

  /** Does parallel merge sort */
  def sorted[B >: A : Ordering : ClassTag]: ParSortedPipeline[B] = new ParSortedPipeline[B](this)

  /**
    * Calculates the sum of pipeline elements in parallel
    * if pipeline elements is a numeric type
    *
    * @param num - a numeric
    * @return - sum of all elements
    **/
  def sum[B >: A](implicit num: Numeric[B]): B = combine(num.zero, num.plus, num.plus)

  /**
    * Combines all pipeline's elements into a single value
    *
    * @tparam C - combiner type
    * @param init  - zero value
    * @param add   - adds an [[A]] to combiner (sequential step of the algorithm)
    * @param merge - merge to [[C]]s into one
    * @return - the resulting combiner itself
    **/
  def combine[C](init: ⇒ C,
                 add: (C, A) ⇒ C,
                 merge: (C, C) ⇒ C): C = {
    val res: C = init
    val forced: Iterable[A] = this.eval
    val combiners: Iterable[Future[C]] = ListUtils.batch(ParDataPipeline.defaultParallelism)(forced)
      .map { group ⇒
        val futureRes = Future {
          group.foldLeft(init)(add)
        }
        futureRes
      }
    val futureRes: Future[Iterable[C]] = Future.sequence(combiners)
    val result: C = Await.result(futureRes, ParDataPipeline.defaultTimeout).foldLeft(res)(merge)
    result
  }

  /**
    * Does the same as [[combine]] above
    * having a [[Monoid]] defined for type [[C]]
    *
    * @tparam C - combiner type
    * @param add     - add an [[A]] to the combiner
    * @param cMonoid - Monoid for the combiner
    * @return - the resulting combiner itself
    **/
  def combine[C](add: (C, A) ⇒ C)(implicit cMonoid: Monoid[C]): C = combine(cMonoid.empty, add, cMonoid.combine)
}

/** Default parameters for parallel data pipeline operations */
object ParDataPipeline {
  val defaultTimeout    : FiniteDuration = 5.minutes
  val defaultParallelism: Int            = Runtime.getRuntime.availableProcessors()
}
