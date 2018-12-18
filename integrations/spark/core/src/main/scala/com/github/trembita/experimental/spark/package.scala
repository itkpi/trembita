package com.github.trembita.experimental

import cats.effect.IO
import cats.{~>, Eval, Functor, Id, Monad, StackSafeMonad}
import com.github.trembita.operations.{CanSort, InjectTaggedK, MagnetF}

import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import com.github.trembita.DataPipelineT
import com.github.trembita.fsm.{FSM, InitialState}
import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql.{AggDecl, AggRes, GroupingCriteria, QueryBuilder, QueryResult}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

package object spark {
  implicit val runIdOnSpark: RunOnSpark[cats.Id] = new RunIdOnSpark

  implicit def runFutureOnSpark(
      implicit timeout: AsyncTimeout
  ): RunOnSpark[Future] =
    new RunFutureOnSpark(timeout)

  implicit def runIOOnSpark(implicit timeout: AsyncTimeout): RunOnSpark[IO] =
    new RunIOOnSpark(timeout)

  implicit class SparkOps[F[_], A](val `this`: DataPipelineT[F, A, Spark]) extends AnyVal with MagnetlessSparkBasicOps[F, A] {
    def evalWith(run: Spark#Run[F])(implicit F: Functor[F], Ex: Spark): F[Vector[A]] =
      `this`.eval(F, Ex, run)

    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
        queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]
    )(implicit trembitaqlForSpark: trembitaqlForSpark[A, G, T, R, Comb],
      run: Spark#Run[F],
      F: Monad[F],
      A: ClassTag[A]): DataPipelineT[F, QueryResult[A, G, R], Spark] =
      `this`.mapRepr(trembitaqlForSpark.apply(_, queryF))
  }

  implicit class SparkIOOps[A](val `this`: DataPipelineT[IO, A, Spark]) extends AnyVal with MagnetlessSparkIOOps[A]

  implicit def materializeFuture[A, B](
      f: A => Future[B]
  ): MagnetF[Future, A, B, Spark] = macro rewrite.materializeFutureImpl[A, B]

  implicit def turnVectorIntoRDD(
      implicit sc: SparkContext
  ): InjectTaggedK[Vector, RDD] = new InjectTaggedK[Vector, RDD] {
    def apply[A: ClassTag](fa: Vector[A]): RDD[A] = sc.parallelize(fa)
  }

  implicit def turnVectorIntoRDD2(
      implicit spark: SparkSession
  ): InjectTaggedK[Vector, RDD] = turnVectorIntoRDD(spark.sparkContext)

  implicit val turnRDDIntoVector: InjectTaggedK[RDD, Vector] =
    InjectTaggedK.fromArrow[RDD, Vector](
      λ[RDD[?] ~> Vector[?]](_.collect().toVector)
    )

  implicit def turnParVectorIntoRDD(
      implicit sc: SparkContext
  ): InjectTaggedK[ParVector, RDD] = new InjectTaggedK[ParVector, RDD] {
    def apply[A: ClassTag](fa: ParVector[A]): RDD[A] = sc.parallelize(fa.seq)
  }

  implicit val turnRDDIntoParVector: InjectTaggedK[RDD, ParVector] =
    InjectTaggedK.fromArrow[RDD, ParVector](
      λ[RDD[?] ~> ParVector[?]](_.collect().toVector.par)
    )

  @transient implicit lazy val globalSafeEc: ExecutionContext =
    ExecutionContext.global

  @transient lazy implicit val safeInstances
    : SerializableMonadError[Future] with SerializableCoflatMap[Future] with SerializableMonad[Future] =
    new SerializableMonadError[Future] with SerializableCoflatMap[Future] with SerializableMonad[Future] with StackSafeMonad[Future] {
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

  implicit val canSortRDD: CanSort[RDD] = new CanSort[RDD] {
    def sorted[A: Ordering: ClassTag](fa: RDD[A]): RDD[A] = fa.sortBy(identity)
    def sortedBy[A: ClassTag, B: Ordering: ClassTag](fa: RDD[A])(f: A => B): RDD[A] =
      fa.sortBy(f)
  }

  implicit class SparkFsmByKey[F[_], A](
      private val self: DataPipelineT[F, A, Spark]
  ) extends AnyVal {
    def fsmByKey[K: Encoder: ClassTag, N: Encoder, D: Encoder, B: ClassTag: TypeTag: Encoder](getKey: A => K)(
        initial: InitialState[N, D, F]
    )(fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B])(
        implicit sparkFSM: SparkFSM[F],
        A: ClassTag[A],
        AEnc: Encoder[A],
        F: SerializableMonad[F],
        run: Spark#Run[F]
    ): DataPipelineT[F, B, Spark] =
      self.mapRepr[B](sparkFSM.byKey[A, K, N, D, B](_)(getKey, initial)(fsmF))
  }

  implicit def runIODsl(timeout: AsyncTimeout): RunOnSpark[IO] =
    new RunIOOnSpark(timeout)

  implicit class DatasetOps[A](private val self: Dataset[A]) extends AnyVal {
    def filterIf(cond: Boolean)(p: Column): Dataset[A] =
      if (cond) self.filter(p)
      else self

    def filterMany(cond0: Column, rest: Column*): Dataset[A] =
      rest.foldLeft(self.filter(cond0))(_ filter _)
  }
}
