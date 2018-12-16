package com.github.trembita.experimental

import cats.instances.FutureCoflatMap
import cats.{CoflatMap, Eval, Monad, MonadError, StackSafeMonad}
import scala.language.experimental.macros
import scala.language.implicitConversions
import com.github.trembita.{InjectTaggedK, MagnetM}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

package object spark {
  implicit val runIdOnSpark: RunOnSpark[cats.Id] = new RunIdOnSpark

  implicit def runFutureOnSpark(implicit timeout: Timeout): RunOnSpark[Future] =
    new RunFutureOnSpark(timeout)

  implicit def materializeFuture[A, B](
    f: A => Future[B]
  ): MagnetM[Future, A, B, Spark] = macro rewrite.materializeFutureImpl[A, B]

  implicit def turnVectorIntoRDD(
    implicit sc: SparkContext
  ): InjectTaggedK[Vector, RDD] = new InjectTaggedK[Vector, RDD] {
    def apply[A: ClassTag](fa: Vector[A]): RDD[A] = sc.parallelize(fa)
  }

  implicit val turnRDDIntoVector: InjectTaggedK[RDD, Vector] =
    new InjectTaggedK[RDD, Vector] {
      def apply[A: ClassTag](fa: RDD[A]): Vector[A] = fa.collect().toVector
    }

  implicit def turnParVectorIntoRDD(
    implicit sc: SparkContext
  ): InjectTaggedK[ParVector, RDD] = new InjectTaggedK[ParVector, RDD] {
    def apply[A: ClassTag](fa: ParVector[A]): RDD[A] = sc.parallelize(fa.seq)
  }

  implicit val turnRDDIntoParVector: InjectTaggedK[RDD, ParVector] =
    new InjectTaggedK[RDD, ParVector] {
      def apply[A: ClassTag](fa: RDD[A]): ParVector[A] =
        fa.collect().toVector.par
    }

  @transient implicit lazy val globalSafeEc: ExecutionContext =
    ExecutionContext.global

  @transient lazy implicit val safeInstances: SerializableMonadError[Future]
    with SerializableCoflatMap[Future]
    with SerializableMonad[Future] =
    new SerializableMonadError[Future] with SerializableCoflatMap[Future]
    with SerializableMonad[Future] with StackSafeMonad[Future] {
      def pure[A](x: A): Future[A] = Future.successful(x)

      def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] =
        fa.flatMap(f)

      def handleErrorWith[A](fea: Future[A])(
        f: Throwable => Future[A]
      ): Future[A] = fea.recoverWith { case t => f(t) }

      def raiseError[A](e: Throwable): Future[A] = Future.failed(e)
      override def handleError[A](fea: Future[A])(
        f: Throwable => A
      ): Future[A] = fea.recover { case t => f(t) }

      override def attempt[A](fa: Future[A]): Future[Either[Throwable, A]] =
        fa.map(a => Right[Throwable, A](a)).recover {
          case NonFatal(t) => Left(t)
        }

      override def recover[A](fa: Future[A])(
        pf: PartialFunction[Throwable, A]
      ): Future[A] = fa.recover(pf)

      override def recoverWith[A](
        fa: Future[A]
      )(pf: PartialFunction[Throwable, Future[A]]): Future[A] =
        fa.recoverWith(pf)

      override def map[A, B](fa: Future[A])(f: A => B): Future[B] = fa.map(f)

      override def catchNonFatal[A](a: => A)(
        implicit ev: Throwable <:< Throwable
      ): Future[A] = Future(a)

      override def catchNonFatalEval[A](a: Eval[A])(
        implicit ev: Throwable <:< Throwable
      ): Future[A] = Future(a.value)

      def coflatMap[A, B](fa: Future[A])(f: Future[A] => B): Future[B] =
        Future(f(fa))
    }
}
