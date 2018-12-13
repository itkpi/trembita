package com.github.trembita.experimental

import scala.language.experimental.macros
import com.github.trembita.MagnetM
import org.apache.spark.SparkContext
import scala.concurrent.Future

package object spark {
  implicit val runIdOnSpark: RunOnSpark[cats.Id] = new RunIdOnSpark

  implicit def runFutureOnSpark(implicit timeout: Timeout): RunOnSpark[Future] = new RunFutureOnSpark(timeout)

  implicit def deriveSpark(implicit sc: SparkContext): Spark = Spark.derive(sc)

  implicit def materializeFuture[A, B](f: A => Future[B]): MagnetM[Future, A, B, Spark] = macro rewrite.materializeFutureImpl[A, B]
}
