package com.github.trembita.spark

import cats.Id
import org.apache.spark.rdd.RDD

import scala.concurrent.Future
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

trait RunOnSpark[F[_]] {
  def runFunc[A, B](a: A)(f: A => F[B]): B
  def lift[A](rdd: RDD[A]): F[RDD[A]]
}

object RunOnSpark {
  implicit val runIdOnSpark: RunOnSpark[Id] = new RunOnSpark[Id] {
    def runFunc[A, B](a: A)(f: A => Id[B]): B = f(a)
    def lift[A](rdd: RDD[A]): Id[RDD[A]] = rdd
  }
  implicit val runFutureOnSpark: RunOnSpark[Future] = new RunOnSpark[Future] {
    def runFunc[A, B](a: A)(f: A => Future[B]): B =
      macro RunFutureOnSpark.runFutureImpl[A, B]
    def lift[A](rdd: RDD[A]): Future[RDD[A]] = Future.successful(rdd)
  }
}

object RunFutureOnSpark {
  def runFutureImpl[A, B](
    c: blackbox.Context
  )(a: c.Expr[A])(f: c.Expr[A => Future[B]]): c.Expr[B] = {
    import c.universe._

    c.Expr[B](q"""
          import scala.concurrent.duration._
          import scala.concurrent.Await
          Await.result($f($a), 5.seconds)
       """)
  }
}
