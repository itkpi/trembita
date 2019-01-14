package com.github.trembita.caching

import java.util.concurrent.CompletableFuture
import java.util.function

import scala.concurrent.Future
import scala.compat.java8.FutureConverters._

package object infinispan {
  implicit class ToScalaFutureOpt[A](private val self: CompletableFuture[A]) extends AnyVal {
    def toScalaOpt: Future[Option[A]] =
      try self.thenApply[Option[A]](new function.Function[A, Option[A]] { override def apply(t: A): Option[A] = Option(t) }).toScala
      catch {
        case e: NullPointerException => Future.successful(None)
      }
  }
}
